#![allow(dead_code, non_snake_case)]

mod generated {
    include!("names.rs");
}
mod raw_delimiters {
    include!("raw_delimiters.rs");
}

use generated::*;
use runtime_sdk::{BusinessError, CallOptions, HostOptions, RuntimeHost};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

fn convert<T: Serialize, U: DeserializeOwned>(value: T) -> U {
    serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap()
}

fn payload() -> Value {
    serde_json::from_str(include_str!("payload.json")).unwrap()
}

fn check_serde() {
    let method = format!("\"{}", "#".repeat(255));
    let property = format!("\"{}", "#".repeat(260));
    assert!(raw_delimiters::contract()["methods"].get(&method).is_some());
    let long_key = json!({ (property): "wire" });
    let long_input: raw_delimiters::FieldInput = serde_json::from_value(long_key.clone()).unwrap();
    assert_eq!(serde_json::to_value(long_input).unwrap(), long_key);

    let original = payload();
    let input: EchoNamesInput = serde_json::from_value(original.clone()).unwrap();
    assert_eq!(input.first_name, "value:first-name");
    assert_eq!(input.r#try, "value:try");
    assert_eq!(input.r#mut, "value:mut");
    assert_eq!(input.r#gen, "value:gen");
    assert_eq!(input.self_, "value:self");
    assert_eq!(input.Self_, "value:Self");
    assert_eq!(input.crate_, "value:crate");
    assert_eq!(input._123value, "value:123value");
    assert_eq!(input.field, "value:");
    assert_eq!(input.field_, "value:_");
    assert_eq!(input.__proto__, "value:__proto__");
    assert_eq!(input.nested_value.child_name, 7);
    assert_eq!(input.items_list[0].item_code, "one");
    assert!(input.optional_value.is_none());
    assert!(input.nullable_value.is_none());
    assert_eq!(serde_json::to_value(input).unwrap(), original);

    let mut absent = original.clone();
    absent.as_object_mut().unwrap().remove("nullable-value");
    assert!(serde_json::from_value::<EchoNamesInput>(absent).is_err());
    let mut null_optional = original.clone();
    null_optional["optional-value"] = Value::Null;
    assert!(serde_json::from_value::<EchoNamesInput>(null_optional).is_err());
    let mut present = original.clone();
    present["optional-value"] = json!(9);
    present["nullable-value"] = json!({"enum": "present"});
    let present_input: EchoNamesInput = serde_json::from_value(present.clone()).unwrap();
    assert_eq!(present_input.optional_value, Some(9));
    assert_eq!(
        present_input.nullable_value.as_ref().unwrap().r#enum,
        "present"
    );
    assert_eq!(serde_json::to_value(present_input).unwrap(), present);

    let mut wrong_name = original;
    let value = wrong_name
        .as_object_mut()
        .unwrap()
        .remove("first-name")
        .unwrap();
    wrong_name["first_name"] = value;
    assert!(serde_json::from_value::<EchoNamesInput>(wrong_name).is_err());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    check_serde();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let kernel = tokio::spawn(kernel::serve(listener, async {
        let _ = stopped.await;
    }));
    let handlers = NamesServer::new()
        .echo_names(|input, _| async move { Ok(convert(input)) })
        .r#try(|input, _| async move {
            if input {
                Err(BusinessError(TryError {
                    error_code: "expected".into(),
                    self_: "wire".into(),
                }))
            } else {
                Ok("ok".into())
            }
        })
        .stream_items(|input, _, output| async move { output.send(convert(input)).await })
        .constructor_(|input, _| async move { Ok(input) })
        .__proto___(|input, _| async move { Ok(input) })
        .new_(|input, _| async move { Ok(input) })
        .build_(|input, _| async move { Ok(input) })
        .control______b(|input, _| async move { Ok(input) })
        .build()?;
    let host = RuntimeHost::connect(
        HostOptions {
            namespace: "identifiers".into(),
            service: "rust".into(),
            instance: "one".into(),
            port,
            contract: contract().clone(),
            max_concurrency: 8,
        },
        handlers,
    )
    .await?;
    println!("{}", json!({"port": port}));

    let mut go = String::new();
    BufReader::new(tokio::io::stdin())
        .read_line(&mut go)
        .await?;
    assert_eq!(go.trim(), "go");
    let client = NamesClient::new(host.clone(), "identifiers", "typescript");
    let original = payload();
    let input: EchoNamesInput = serde_json::from_value(original.clone())?;
    let output = client.echo_names(input, CallOptions::default()).await?;
    assert_eq!(serde_json::to_value(output)?, original);
    assert_eq!(client.r#try(false, CallOptions::default()).await?, "ok");
    let error = client
        .r#try(true, CallOptions::default())
        .await
        .unwrap_err();
    let details: TryError = error.business_details().unwrap();
    assert_eq!(details.error_code, "expected");
    assert_eq!(details.self_, "wire");
    let mut stream = client.stream_items(
        serde_json::from_value(original.clone())?,
        CallOptions::default(),
    )?;
    assert_eq!(
        serde_json::to_value(stream.next().await?.unwrap())?,
        original
    );
    assert!(stream.next().await?.is_none());
    assert_eq!(
        client
            .constructor_("constructor".into(), CallOptions::default())
            .await?,
        "constructor"
    );
    assert_eq!(
        client
            .__proto___("proto".into(), CallOptions::default())
            .await?,
        "proto"
    );
    assert_eq!(
        client.new_("new".into(), CallOptions::default()).await?,
        "new"
    );
    assert_eq!(
        client
            .build_("build".into(), CallOptions::default())
            .await?,
        "build"
    );
    assert_eq!(
        client
            .control______b("controls".into(), CallOptions::default())
            .await?,
        "controls"
    );
    println!("{}", json!({"rust_client": "pass", "serde": "pass"}));
    host.close();
    let _ = shutdown.send(());
    kernel.await??;
    Ok(())
}
