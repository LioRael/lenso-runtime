use runtime_sdk::{Handlers, HostOptions, RuntimeHost, Transport};
use serde_json::{json, Value};
use std::{process::Stdio, time::Duration};
use tokio::{process::Command, time::timeout};
use transport::Connection;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUNTIME_STDIO_TEST_CHILD").is_some() {
        let host = RuntimeHost::connect_with_transport(
            HostOptions {
                namespace: "demo".into(),
                service: "stdio".into(),
                instance: "one".into(),
                port: 7331,
                contract: json!({"version":"1","methods":{}}),
                max_concurrency: 1,
            },
            Handlers::new(),
            Transport::Stdio,
        )
        .await
        .unwrap();
        host.drain(1000).await.unwrap();
        host.close();
        return;
    }
    timeout(Duration::from_secs(10), async {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .env("RUNTIME_STDIO_TEST_CHILD", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut connection =
            Connection::pipes(child.stdout.take().unwrap(), child.stdin.take().unwrap());
        for (expected, response) in [
            (
                "hello",
                json!({"type":"welcome","version":1,"generation":"1","heartbeat_ms":1000}),
            ),
            ("register", json!({"type":"registered"})),
            ("ready", json!({"type":"ready"})),
            ("drain", json!({"type":"drained"})),
        ] {
            loop {
                let payload = connection.reader.recv().await.unwrap().unwrap();
                let frame: Value = serde_json::from_slice(&payload).unwrap();
                if frame["type"] == "heartbeat" {
                    continue;
                }
                assert_eq!(frame["type"], expected);
                break;
            }
            connection
                .writer
                .send(&serde_json::to_vec(&response).unwrap())
                .await
                .unwrap();
        }
        // Tokio's blocking stdin reader needs EOF before the child runtime can shut down.
        connection.writer.close().await.unwrap();
        drop(connection);
        assert!(child.wait().await.unwrap().success());
    })
    .await
    .unwrap();
}
