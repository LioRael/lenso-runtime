//! Owned service processes, independent of protocol sessions and readiness.
//!
//! Call `stop` on shutdown or failed readiness, even if `try_wait` reports an
//! exited leader. Unix children get a dedicated process group; shutdown targets
//! that group, including surviving descendants. Descendants that create their
//! own sessions/groups escape this boundary. We can reap only our direct child,
//! not orphan grandchildren (their OS reaper owns them). Group signalling is
//! best effort and has the usual numeric process-group ID reuse race.
//!
//! Drop sends an immediate kill without waiting; Tokio's `kill_on_drop` is the
//! direct-child fallback. Keep the Tokio runtime alive and await `stop` for
//! deterministic reaping. On non-Unix platforms only the direct child is
//! terminated; no process-tree or graceful SIGTERM guarantee is provided.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::timeout;

/// Communication selected for the service, not its readiness state.
#[derive(Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProcessTransport {
    #[default]
    Stdio,
    Tcp,
}

/// Executable and expected identity. Environment values are deliberately not
/// exposed through a `Debug` implementation or logged by this module.
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub transport: ProcessTransport,
    pub namespace: String,
    pub service: String,
    pub instance: String,
    /// The parent owns the ready deadline and identity/session validation.
    #[serde(default = "default_ready_timeout_ms")]
    pub ready_timeout_ms: u64,
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
}

impl ProcessConfig {
    /// Validate bounded lifecycle settings before launching any command.
    pub fn validate(&self) -> io::Result<()> {
        if self.command.is_empty()
            || [&self.namespace, &self.service, &self.instance]
                .iter()
                .any(|value| value.is_empty() || value.len() > 256)
            || !(1..=60000).contains(&self.ready_timeout_ms)
            || !(1..=60000).contains(&self.shutdown_timeout_ms)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid managed process configuration",
            ));
        }
        Ok(())
    }
}

fn default_ready_timeout_ms() -> u64 {
    5000
}

fn default_shutdown_timeout_ms() -> u64 {
    2000
}

/// A child process, which may exist before or after its protocol session.
pub struct ManagedProcess {
    /// Stdout reader/stdin writer for stdio; TCP sessions arrive separately.
    pub connection: Option<transport::Connection>,
    /// Original child PID, retained after exit for lifecycle reporting.
    pub pid: u32,
    child: Child,
    shutdown_timeout: Duration,
    stopped: bool,
}

/// Spawn one executable directly, without shell parsing or readiness probing.
///
/// Requires an active Tokio runtime. Environment overrides inherit the parent's
/// environment; the transport selector is always set after user overrides.
/// The TCP port is supplied by the parent through `config.env`. Stdio stdout
/// is reserved for protocol frames; stderr is inherited in either mode.
pub fn spawn(config: &ProcessConfig) -> io::Result<ManagedProcess> {
    config.validate()?;
    let mut command = Command::new(&config.command);
    command
        .args(&config.args)
        .envs(&config.env)
        .env(
            "LENSO_RUNTIME_TRANSPORT",
            match config.transport {
                ProcessTransport::Stdio => "stdio",
                ProcessTransport::Tcp => "tcp",
            },
        )
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(cwd) = &config.cwd {
        command.current_dir(cwd);
    }
    match config.transport {
        ProcessTransport::Stdio => {
            command.stdin(Stdio::piped()).stdout(Stdio::piped());
        }
        ProcessTransport::Tcp => {
            command.stdin(Stdio::null()).stdout(Stdio::inherit());
        }
    }
    #[cfg(unix)]
    command.process_group(0);

    let child = command.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("spawned process has no PID"))?;
    // Establish ownership before taking the pipes so every failure kills the group.
    let mut process = ManagedProcess {
        connection: None,
        pid,
        child,
        shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
        stopped: false,
    };
    if config.transport == ProcessTransport::Stdio {
        let stdout = process
            .child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing child stdout pipe"))?;
        let stdin = process
            .child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("missing child stdin pipe"))?;
        process.connection = Some(transport::Connection::pipes(stdout, stdin));
    }
    Ok(process)
}

impl ManagedProcess {
    /// Poll and reap the direct child. An exited leader does not imply its
    /// descendants or its session have stopped; still call `stop`.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Close owned pipes, SIGTERM the Unix group, allow the configured grace
    /// period, then SIGKILL remaining group members and reap the direct child.
    ///
    /// Forced reaping has a separate bounded wait of at least 1 ms, using the
    /// configured shutdown timeout otherwise. If it times out or signalling
    /// fails, return an error and retain Drop's kill fallback. Cancellation of
    /// this future likewise retains ownership: call again or drop the process.
    /// The parent must drain the protocol session before calling this method.
    pub async fn stop(&mut self) -> io::Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.connection.take();
        #[cfg(unix)]
        let graceful_signal = signal_group(self.pid, libc::SIGTERM).map(|_| ());
        #[cfg(not(unix))]
        let graceful_signal = self.child.start_kill();

        if graceful_signal.is_ok() {
            let graceful = async {
                self.child.wait().await?;
                #[cfg(unix)]
                while signal_group(self.pid, 0)? {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, io::Error>(())
            };
            if let Ok(Ok(())) = timeout(self.shutdown_timeout, graceful).await {
                self.stopped = true;
                return Ok(());
            }
        }

        // Always attempt both group cleanup and child reaping, even if a signal
        // failed. Do not mark stopped until both operations succeeded.
        #[cfg(unix)]
        let forced_signal = signal_group(self.pid, libc::SIGKILL).map(|_| ());
        #[cfg(not(unix))]
        let forced_signal = self.child.start_kill();
        let direct_kill = self.child.start_kill();
        let reaped = timeout(
            self.shutdown_timeout.max(Duration::from_millis(1)),
            self.child.wait(),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "process reap timed out"))?;
        reaped?;
        forced_signal?;
        direct_kill?;
        graceful_signal?;
        self.stopped = true;
        Ok(())
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if !self.stopped {
            #[cfg(unix)]
            if let Err(error) = signal_group(self.pid, libc::SIGKILL) {
                crate::diagnostics::emit(
                    serde_json::json!({"event":"process_drop_group_kill_failed","pid":self.pid,"error":error.to_string()}),
                );
            }
            // Child also has kill_on_drop, including when stop was cancelled.
            if let Err(error) = self.child.start_kill() {
                crate::diagnostics::emit(
                    serde_json::json!({"event":"process_drop_kill_failed","pid":self.pid,"error":error.to_string()}),
                );
            }
        }
    }
}

/// Returns false if the group no longer exists.
#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) -> io::Result<bool> {
    let group = libc::pid_t::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid process group ID"))?;
    if group <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid process group ID",
        ));
    }
    // SAFETY: kill takes only integers. Negating this positive, owned child PID
    // selects its dedicated group, never the parent's group or all processes.
    if unsafe { libc::kill(-group, signal) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(command: &str, args: &[&str]) -> ProcessConfig {
        ProcessConfig {
            command: command.into(),
            args: args.iter().map(|arg| (*arg).into()).collect(),
            cwd: None,
            env: BTreeMap::new(),
            transport: ProcessTransport::Stdio,
            namespace: "test".into(),
            service: "process".into(),
            instance: "one".into(),
            ready_timeout_ms: 5000,
            shutdown_timeout_ms: 100,
        }
    }

    #[test]
    fn configuration_defaults_and_transport_names() {
        let config: ProcessConfig = serde_json::from_value(serde_json::json!({
            "command": "service", "namespace": "test",
            "service": "process", "instance": "one"
        }))
        .unwrap();
        assert!(config.args.is_empty());
        assert!(config.env.is_empty());
        assert!(config.cwd.is_none());
        assert!(config.transport == ProcessTransport::Stdio);
        assert_eq!(config.ready_timeout_ms, 5000);
        assert_eq!(config.shutdown_timeout_ms, 2000);
        assert!(
            serde_json::from_str::<ProcessTransport>("\"tcp\"").unwrap() == ProcessTransport::Tcp
        );
    }

    #[tokio::test]
    async fn spawn_failure_returns_not_found() {
        let result = spawn(&config("/nonexistent-lenso-test-executable", &[]));
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::NotFound));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn early_exit_is_reaped_and_stop_is_idempotent() {
        let mut process = spawn(&config("/bin/sh", &["-c", "exit 7"])).unwrap();
        let status = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(status) = process.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(status.code(), Some(7));
        process.stop().await.unwrap();
        process.stop().await.unwrap();
        assert!(process.try_wait().unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forced_stop_kills_term_ignoring_child_and_reaps() {
        let mut process = spawn(&config(
            "/bin/sh",
            &["-c", "trap '' TERM; echo armed; exec sleep 30"],
        ))
        .unwrap();
        // Wait for the trap to be installed rather than racing SIGTERM.
        let armed = timeout(
            Duration::from_secs(2),
            process.connection.as_mut().unwrap().reader.recv(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(String::from_utf8(armed).unwrap().trim(), "armed");
        timeout(Duration::from_secs(2), process.stop())
            .await
            .unwrap()
            .unwrap();
        assert!(process.try_wait().unwrap().is_some());
        assert!(!signal_group(process.pid, 0).unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stop_kills_descendant_after_leader_was_reaped() {
        let mut process = spawn(&config(
            "/bin/sh",
            &["-c", "trap '' TERM; sleep 30 & echo \"$!\"; exit 0"],
        ))
        .unwrap();
        let frame = timeout(
            Duration::from_secs(2),
            process.connection.as_mut().unwrap().reader.recv(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let descendant: u32 = String::from_utf8(frame).unwrap().trim().parse().unwrap();
        timeout(Duration::from_secs(2), process.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(signal_group(process.pid, 0).unwrap());
        process.stop().await.unwrap();
        // Orphan grandchildren can remain zombies until the OS reaper runs;
        // verify they are not executing, rather than claiming we reaped them.
        timeout(Duration::from_secs(2), async {
            loop {
                let output = Command::new("ps")
                    .args(["-o", "stat=", "-p", &descendant.to_string()])
                    .output()
                    .await
                    .unwrap();
                let state = String::from_utf8(output.stdout).unwrap();
                if state.trim().is_empty() || state.trim().starts_with('Z') {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn graceful_stop_terminates_group_and_parent_reaps_descendant() {
        let mut process = spawn(&config(
            "/bin/sh",
            &[
                "-c",
                "sleep 30 & kid=$!; trap 'wait \"$kid\"; exit 0' TERM; echo armed; wait \"$kid\"",
            ],
        ))
        .unwrap();
        timeout(
            Duration::from_secs(2),
            process.connection.as_mut().unwrap().reader.recv(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        process.stop().await.unwrap();
        assert!(!signal_group(process.pid, 0).unwrap());
    }
}
