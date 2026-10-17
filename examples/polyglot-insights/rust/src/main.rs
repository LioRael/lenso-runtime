#![allow(dead_code)] // Generated contracts include clients/servers for other peers.
use runtime_sdk::{
    CallOptions, HandlerContext, HostOptions, RuntimeError, RuntimeHost, StreamSender,
};
use serde_json::json;
use std::time::Duration;

mod gateway {
    include!("../../generated/gateway.rs");
}
mod metrics {
    include!("../../generated/metrics.rs");
}
mod cleaner {
    include!("../../generated/cleaner.rs");
}

fn child_options(context: &HandlerContext) -> CallOptions {
    context.call_options()
}

// Drop also runs when the SDK aborts a cancelled handler. This is application
// evidence of released work, independent of the kernel's retired route count.
struct WorkLog {
    context: HandlerContext,
    sent: usize,
    completed: bool,
}
impl Drop for WorkLog {
    fn drop(&mut self) {
        eprintln!(
            "{}",
            json!({"event":"analyze_finished", "call_id":self.context.call_id,
            "trace_id":self.context.trace_id, "sent":self.sent, "completed":self.completed,
            "cancelled":self.context.cancellation.is_cancelled()})
        );
    }
}

async fn analyze(
    host: RuntimeHost,
    namespace: String,
    input: metrics::AnalyzeInput,
    context: HandlerContext,
    output: StreamSender<metrics::AnalyzeOutput>,
) -> Result<(), RuntimeError> {
    let mut work = WorkLog {
        context: context.clone(),
        sent: 0,
        completed: false,
    };
    eprintln!(
        "{}",
        json!({"event":"analyze_started", "trace_id":context.trace_id,
        "call_id":context.call_id, "caller":context.caller})
    );
    if input.records.is_empty() {
        return Err(RuntimeError::business(metrics::AnalyzeError {
            reason: "No valid requests to analyze".into(),
        }));
    }
    let policy = gateway::GatewayClient::new(host.clone(), &namespace, "gateway")
        .policy(gateway::PolicyInput {}, child_options(&context))
        .await?;
    let total = input.records.len();
    let mut durations = Vec::with_capacity(total);
    let mut errors = 0;
    let mut slow = 0;
    let mut sum = 0;
    for (index, record) in input.records.iter().enumerate() {
        tokio::select! {
            _ = context.cancellation.cancelled() => return Err(RuntimeError::new("cancelled")),
            _ = tokio::time::sleep(Duration::from_millis(input.delay_ms.unwrap_or(0) as u64)) => (),
        }
        durations.push(record.duration_ms);
        errors += i64::from(record.status >= 500);
        slow += i64::from(record.duration_ms >= policy.slow_ms);
        sum += record.duration_ms;
        let processed = index + 1;
        if processed % input.batch_size.unwrap_or(2) as usize != 0 && processed != total {
            continue;
        }
        let mut sorted = durations.clone();
        sorted.sort_unstable();
        let p95 = sorted[(95 * processed).div_ceil(100) - 1];
        let mut chunk = metrics::AnalyzeOutput {
            processed: processed as i64,
            total: total as i64,
            error_count: errors,
            slow_count: slow,
            total_duration_ms: sum,
            p95_ms: p95,
            report: String::new(),
        };
        if processed == total {
            let report = cleaner::CleanerClient::new(host.clone(), &namespace, "cleaner")
                .render(
                    cleaner::RenderInput {
                        processed: chunk.processed,
                        total: chunk.total,
                        error_count: errors,
                        slow_count: slow,
                        total_duration_ms: sum,
                        p95_ms: p95,
                    },
                    child_options(&context),
                )
                .await?;
            chunk.report = report.text;
        }
        output.send(chunk).await?;
        work.sent += 1;
        eprintln!(
            "{}",
            json!({"event":"chunk_sent", "trace_id":context.trace_id, "processed":processed})
        );
    }
    work.completed = true;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::var("INSIGHTS_NAMESPACE")?;
    let host = RuntimeHost::connect_registered(HostOptions {
        namespace: namespace.clone(),
        service: "metrics".into(),
        instance: "rust-one".into(),
        port: std::env::var("LENSO_RUNTIME_PORT")?.parse()?,
        contract: metrics::contract().clone(),
        max_concurrency: 8,
    })
    .await?;
    let callback_host = host.clone();
    let handlers = metrics::MetricsServer::new()
        .limits(|_, context| async move {
            eprintln!(
                "{}",
                json!({"event":"limits", "caller":context.caller, "trace_id":context.trace_id,
                "parent_id":context.parent_id})
            );
            Ok(metrics::LimitsOutput {
                max_records: 1000,
                max_duration_ms: 60000,
            })
        })
        .analyze(move |input, context, output| {
            analyze(
                callback_host.clone(),
                namespace.clone(),
                input,
                context,
                output,
            )
        })
        .build()?;
    host.activate(handlers).await?;
    eprintln!("{}", json!({"event":"metrics_ready"}));
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => (),
            _ = host.wait_closed() => { host.close(); return Ok(()); },
        }
    }
    #[cfg(not(unix))]
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = host.wait_closed() => { host.close(); return Ok(()); },
    }
    let drained = host.drain(1000).await;
    host.close();
    drained?;
    Ok(())
}
