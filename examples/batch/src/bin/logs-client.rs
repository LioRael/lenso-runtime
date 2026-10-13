use runtime_sdk::{CallOptions, Handlers, HostOptions, RuntimeHost};
pub mod logs {
    include!("../../../generated/logs.rs");
}
use logs::{AggregateInput, AggregateInputLogsItem, LogsClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("LENSO_RUNTIME_PORT")
        .unwrap_or_else(|_| "7331".into())
        .parse()?;
    let host = RuntimeHost::connect(
        HostOptions {
            namespace: "demo".into(),
            service: "rust-client".into(),
            instance: "one".into(),
            port,
            contract: serde_json::json!({"version":"1","methods":{}}),
            max_concurrency: 1,
        },
        Handlers::new(),
    )
    .await?;
    let service = std::env::var("LENSO_RUNTIME_SERVICE").unwrap_or_else(|_| "batch".into());
    let client = LogsClient::new(host.clone(), "demo", service);
    let result = client
        .aggregate(
            AggregateInput {
                logs: vec![
                    AggregateInputLogsItem {
                        level: "info".into(),
                        message: "synthetic startup".into(),
                        duration_ms: 3,
                    },
                    AggregateInputLogsItem {
                        level: "error".into(),
                        message: "synthetic failure".into(),
                        duration_ms: 7,
                    },
                ],
            },
            CallOptions::default(),
        )
        .await;
    let drained = host.drain(5000).await;
    host.close();
    println!("{}", serde_json::to_string(&result?)?);
    drained?;
    Ok(())
}
