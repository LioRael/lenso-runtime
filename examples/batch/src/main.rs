use runtime_sdk::{
    BusinessError, HandlerContext, Handlers, HostOptions, RuntimeError, RuntimeHost, StreamSender,
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
    let mut handlers = Handlers::new();
    handlers.unary("aggregate", aggregate);
    handlers.stream("progress", progress);
    let host = RuntimeHost::connect(
        HostOptions {
            namespace: "demo".into(),
            service: "batch".into(),
            instance: "rust-one".into(),
            port,
            contract: serde_json::from_str(include_str!("../../contracts/logs.json"))?,
            max_concurrency: 8,
        },
        handlers,
    )
    .await?;
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
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    let result = host.drain(5000).await;
    host.close();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
