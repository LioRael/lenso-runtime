//! Invoked by scripts/sdk-error-probes/run.py against its owned kernel/service.
use runtime_sdk::{CallOptions, Cancellation, Handlers, HostOptions, RuntimeHost, Target};
use serde_json::{json, Value};
use std::time::Duration;
#[tokio::main]
async fn main() {
    let host = RuntimeHost::connect(
        HostOptions {
            namespace: "errors".into(),
            service: "rust".into(),
            instance: "one".into(),
            port: std::env::var("LENSO_RUNTIME_PORT")
                .unwrap()
                .parse()
                .unwrap(),
            contract: json!({"version":"1", "methods":{}}),
            max_concurrency: 8,
        },
        Handlers::new(),
    )
    .await
    .unwrap();
    let failure = RuntimeHost::connect(
        HostOptions {
            namespace: "errors".into(),
            service: "bad-rust".into(),
            instance: "one".into(),
            port: std::env::var("LENSO_PROTOCOL_PORT")
                .unwrap()
                .parse()
                .unwrap(),
            contract: json!({"version":"1", "methods":{}}),
            max_concurrency: 8,
        },
        Handlers::new(),
    )
    .await
    .err()
    .expect("protocol mismatch must fail");
    assert_eq!(failure.code, "protocol_error");
    assert!(failure.details.is_none());
    println!("Rust protocol_error: passed");
    for (version, method, delay, options, code) in [
        ("2", "wait", 0, CallOptions::default(), "contract_mismatch"),
        (
            "1",
            "missing",
            0,
            CallOptions::default(),
            "contract_mismatch",
        ),
        (
            "1",
            "wait",
            500,
            CallOptions {
                timeout_ms: 50,
                ..Default::default()
            },
            "deadline_exceeded",
        ),
    ] {
        let target = Target {
            namespace: "errors".into(),
            service: "server".into(),
            contract_version: version.into(),
        };
        let error = host
            .call::<_, Value>(&target, method, delay, options)
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
        assert!(error.details.is_none());
        println!("Rust {code}: passed");
    }
    let target = Target {
        namespace: "errors".into(),
        service: "server".into(),
        contract_version: "1".into(),
    };
    let cancellation = Cancellation::new();
    let trigger = cancellation.clone();
    let cancel = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.cancel();
    });
    let error = host
        .call::<_, Value>(
            &target,
            "wait",
            500,
            CallOptions {
                cancellation: Some(cancellation),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    cancel.await.unwrap();
    assert_eq!(error.code, "cancelled");
    assert!(error.details.is_none());
    println!("Rust cancelled: passed");
    assert_eq!(
        host.call::<_, Value>(&target, "wait", 0, CallOptions::default())
            .await
            .unwrap(),
        Value::Null
    );
    host.close();
}
