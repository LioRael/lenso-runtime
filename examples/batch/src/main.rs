use runtime_sdk::{
    BusinessError, HandlerContext, HostOptions, RuntimeError, RuntimeHost, StreamSender, Transport,
};
use std::time::Duration;
pub mod logs {
    include!("../../generated/logs.rs");
}
use logs::*;

async fn aggregate(
    input: AggregateInput,
    context: HandlerContext,
) -> Result<AggregateOutput, BusinessError<AggregateError>> {
    if input.logs.is_empty() {
        return Err(BusinessError(AggregateError {
            reason: "At least one log is required".into(),
        }));
    }
    let mut result = AggregateOutput {
        count: 0,
        error_count: 0,
        total_duration_ms: 0,
    };
    for log in input.logs {
        if context.cancellation.is_cancelled() {
            return Err(BusinessError(AggregateError {
                reason: "Aggregation cancelled".into(),
            }));
        }
        result.count += 1;
        result.error_count += i64::from(log.level == "error");
        result.total_duration_ms += log.duration_ms;
    }
    Ok(result)
}
async fn progress(
    input: ProgressInput,
    context: HandlerContext,
    output: StreamSender<ProgressOutput>,
) -> Result<(), RuntimeError> {
    if input.logs.is_empty() {
        return Err(RuntimeError::business(ProgressError {
            reason: "At least one log is required".into(),
        }));
    }
    let total = input.logs.len() as i64;
    for (index, _) in input.logs.iter().enumerate() {
        let delay = input.delay_ms.unwrap_or(0);
        if delay > 0 {
            tokio::select! {
                _ = context.cancellation.cancelled() => return Err(RuntimeError::new("cancelled")),
                _ = tokio::time::sleep(Duration::from_millis(delay as u64)) => (),
            }
        }
        output
            .send(ProgressOutput {
                processed: index as i64 + 1,
                total,
            })
            .await?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("LENSO_RUNTIME_PORT")
        .unwrap_or_else(|_| "7331".into())
        .parse()?;
    let handlers = LogsServer::new()
        .aggregate(aggregate)
        .progress(progress)
        .build()?;
    let transport = match std::env::var("LENSO_RUNTIME_TRANSPORT").as_deref() {
        Ok("stdio") => Transport::Stdio,
        Ok("websocket") => Transport::WebSocket {
            url: std::env::var("LENSO_RUNTIME_URL")?,
        },
        Ok("tcp") | Err(_) => Transport::Tcp,
        Ok(value) => return Err(format!("Unsupported runtime transport: {value}").into()),
    };
    let host = RuntimeHost::connect_with_transport(
        HostOptions {
            namespace: std::env::var("LENSO_RUNTIME_NAMESPACE").unwrap_or_else(|_| "demo".into()),
            service: std::env::var("LENSO_RUNTIME_SERVICE").unwrap_or_else(|_| "batch".into()),
            instance: std::env::var("LENSO_RUNTIME_INSTANCE").unwrap_or_else(|_| "rust-one".into()),
            port,
            contract: serde_json::from_str(include_str!("../../contracts/logs.json"))?,
            max_concurrency: 8,
        },
        handlers,
        transport,
    )
    .await?;
    if let Ok(path) = std::env::var("TRANSPORT_PID_FILE") {
        std::fs::write(path, std::process::id().to_string())?;
    }
    eprintln!(
        "{}",
        serde_json::json!({"event":"service_ready","namespace":"demo","service":"batch","port":port})
    );
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => (),
            _ = host.wait_closed() => (),
        }
    }
    #[cfg(not(unix))]
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = host.wait_closed() => (),
    }
    let result = host.drain(5000).await;
    host.close();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_server_infers_closure_types_and_rejects_incomplete_or_duplicate_bindings() {
        assert_eq!(
            LogsServer::new().build().err().unwrap().code,
            "contract_mismatch"
        );
        let server = LogsServer::new()
            .aggregate(|input, _context| async move {
                Ok(AggregateOutput {
                    count: input.logs.len() as i64,
                    error_count: 0,
                    total_duration_ms: 0,
                })
            })
            .progress(|input, _context, output| async move {
                output
                    .send(ProgressOutput {
                        processed: 0,
                        total: input.logs.len() as i64,
                    })
                    .await
            });
        assert!(server.build().is_ok());
        assert_eq!(
            LogsServer::new()
                .aggregate(aggregate)
                .aggregate(aggregate)
                .progress(progress)
                .build()
                .err()
                .unwrap()
                .code,
            "contract_mismatch"
        );
    }
    #[test]
    fn generated_optional_integer_rejects_null_but_accepts_absence() {
        assert!(serde_json::from_str::<ProgressInput>(r#"{"logs":[]}"#)
            .unwrap()
            .delay_ms
            .is_none());
        assert!(serde_json::from_str::<ProgressInput>(r#"{"logs":[],"delay_ms":null}"#).is_err());
        assert!(serde_json::from_str::<ProgressInput>(r#"{}"#).is_err());
    }
    #[tokio::test]
    async fn aggregate_counts_structured_logs_and_returns_typed_business_error() {
        let context = HandlerContext {
            cancellation: runtime_sdk::Cancellation::new(),
            call_id: "one".into(),
            trace_id: "trace".into(),
            parent_id: None,
            caller: serde_json::Value::Null,
        };
        let input = serde_json::from_str(r#"{"logs":[{"level":"error","message":"failed","duration_ms":12},{"level":"info","message":"done","duration_ms":8}]}"#).unwrap();
        let output = aggregate(input, context.clone()).await.unwrap();
        assert_eq!(
            (output.count, output.error_count, output.total_duration_ms),
            (2, 1, 20)
        );
        assert_eq!(
            aggregate(AggregateInput { logs: vec![] }, context)
                .await
                .unwrap_err()
                .0
                .reason,
            "At least one log is required"
        );
    }
}
