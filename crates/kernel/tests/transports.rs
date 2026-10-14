use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Host(transport::Connection);

impl Host {
    async fn send(&mut self, value: Value) {
        self.0
            .writer
            .send(&serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Value {
        let bytes = tokio::time::timeout(Duration::from_secs(2), self.0.reader.recv())
            .await
            .unwrap()
            .unwrap()
            .expect("frame");
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn hello(&mut self, service: &str, instance: &str) -> String {
        self.send(json!({"type":"hello","version":1,"namespace":"mixed","service":service,"instance":instance})).await;
        let welcome = self.recv().await;
        assert_eq!(welcome["type"], "welcome");
        welcome["generation"].as_str().unwrap().to_owned()
    }

    async fn ready(&mut self) {
        self.send(json!({"type":"register","max_concurrency":1,"contract":{
            "version":"1","methods":{"run":{"mode":"stream","input":{"type":"integer"},
            "output":{"type":"integer"},"error":{"type":"string"}}}
        }}))
        .await;
        assert_eq!(self.recv().await["type"], "registered");
        self.send(json!({"type":"ready"})).await;
        assert_eq!(self.recv().await["type"], "ready");
    }

    async fn call(&mut self, id: &str, timeout_ms: u64) {
        self.send(
            json!({"type":"call","id":id,"namespace":"mixed","service":"worker",
            "method":"run","contract_version":"1","input":1,"timeout_ms":timeout_ms}),
        )
        .await;
    }
}

#[tokio::test]
async fn websocket_and_tcp_share_credit_cancel_deadline_cutover_and_no_replay() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp.local_addr().unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(kernel::serve_with_transports(
        tcp,
        Some(ws),
        vec![],
        async {
            let _ = stopped.await;
        },
    ));
    let mut caller =
        Host(transport::Connection::tcp(TcpStream::connect(tcp_addr).await.unwrap()).unwrap());
    caller.hello("caller", "tcp").await;
    let mut old = Host(transport::connect_websocket(&ws_url).await.unwrap());
    old.hello("worker", "old").await;
    old.ready().await;
    caller.send(json!({"type":"promote","id":"1","namespace":"mixed","service":"worker","instance":"old","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    caller.call("2", 2000).await;
    let invoke = old.recv().await;
    assert_eq!(invoke["type"], "invoke");
    caller
        .send(json!({"type":"credit","id":"2","credits":1}))
        .await;
    assert_eq!(old.recv().await["type"], "credit");
    old.send(json!({"type":"chunk","id":invoke["id"],"output":7}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"chunk","id":"2","output":7})
    );
    caller.send(json!({"type":"cancel","id":"2"})).await;
    assert_eq!(old.recv().await["type"], "cancel");
    assert_eq!(caller.recv().await["code"], "cancelled");
    // Late and repeated terminal frames never resurrect a retired route.
    old.send(json!({"type":"end","id":invoke["id"]})).await;
    old.send(json!({"type":"end","id":invoke["id"]})).await;
    caller.call("3", 50).await;
    let _ = old.recv().await;
    assert_eq!(caller.recv().await["code"], "deadline_exceeded");
    assert_eq!(old.recv().await["type"], "cancel");

    let mut next =
        Host(transport::Connection::tcp(TcpStream::connect(tcp_addr).await.unwrap()).unwrap());
    let generation = next.hello("worker", "next").await;
    next.ready().await;
    caller.send(json!({"type":"promote","id":"4","namespace":"mixed","service":"worker","instance":"next","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    // A WebSocket caller now routes into the TCP service.
    let mut ws_caller = Host(transport::connect_websocket(&ws_url).await.unwrap());
    ws_caller.hello("caller", "ws").await;
    ws_caller.call("1", 2000).await;
    assert_eq!(next.recv().await["type"], "invoke");
    next.0.writer.close().await.unwrap();
    drop(next);
    assert_eq!(ws_caller.recv().await["code"], "connection_lost");
    let mut reconnect =
        Host(transport::Connection::tcp(TcpStream::connect(tcp_addr).await.unwrap()).unwrap());
    assert_ne!(reconnect.hello("worker", "next").await, generation);
    reconnect.ready().await;
    ws_caller.call("2", 1000).await;
    assert_eq!(ws_caller.recv().await["code"], "not_ready");
    caller.send(json!({"type":"status","id":"5"})).await;
    let status = caller.recv().await;
    assert_eq!(status["calls"].as_array().unwrap().len(), 0);
    assert!(status["services"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["transport"] == "websocket"));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
