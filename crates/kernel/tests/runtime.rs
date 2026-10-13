use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct Server {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(kernel::serve(listener, async {
            let _ = stopped.await;
        }));
        Self {
            address,
            stop,
            task,
        }
    }
    async fn finish(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

struct Host {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    generation: Value,
}

impl Host {
    async fn connect(address: SocketAddr, service: &str) -> Self {
        Self::connect_instance(address, service, "one").await
    }
    async fn connect_instance(address: SocketAddr, service: &str, instance: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let (read, write) = stream.into_split();
        let mut host = Self {
            read: BufReader::new(read),
            write,
            generation: Value::Null,
        };
        host.send(json!({"type":"hello","version":1,"namespace":"demo","service":service,"instance":instance})).await;
        let welcome = host.recv().await;
        assert_eq!(welcome["type"], "welcome");
        host.generation = welcome["generation"].clone();
        host
    }
    async fn send(&mut self, frame: Value) {
        let mut bytes = serde_json::to_vec(&frame).unwrap();
        bytes.push(b'\n');
        self.write.write_all(&bytes).await.unwrap();
    }
    async fn recv(&mut self) -> Value {
        let mut line = String::new();
        let count = tokio::time::timeout(Duration::from_secs(2), self.read.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(count > 0, "unexpected EOF");
        serde_json::from_str(&line).unwrap()
    }
    async fn register(&mut self, mode: &str) {
        self.send(json!({"type":"register","max_concurrency":1,"contract":contract(mode)}))
            .await;
        assert_eq!(self.recv().await["type"], "registered");
        self.send(json!({"type":"ready"})).await;
        assert_eq!(self.recv().await["type"], "ready");
    }
    async fn call(&mut self, id: &str, timeout_ms: u64) {
        self.send(json!({"type":"call","id":id,"namespace":"demo","service":"batch","method":"run","contract_version":"1","input":1,"timeout_ms":timeout_ms})).await;
    }
}

fn contract(mode: &str) -> Value {
    json!({"version":"1","methods":{"run":{"mode":mode,"input":{"type":"integer"},"output":{"type":"integer"},"error":{"type":"string"}}}})
}

#[tokio::test]
async fn promotion_preserves_old_stream_and_routes_waiting_and_new_calls_to_candidate() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut old = Host::connect_instance(server.address, "batch", "old").await;
    old.register("stream").await;
    caller.send(json!({"type":"promote","id":"1","namespace":"demo","service":"batch","instance":"old","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    caller.call("2", 2000).await;
    let active = old.recv().await;
    caller
        .send(json!({"type":"credit","id":"2","credits":1}))
        .await;
    assert_eq!(old.recv().await["type"], "credit");
    caller.call("3", 2000).await;
    caller.send(json!({"type":"status","id":"4"})).await;
    let before = caller.recv().await;
    assert_eq!(before["calls"][1]["state"], "waiting");

    let mut new = Host::connect_instance(server.address, "batch", "new").await;
    new.register("stream").await;
    caller.send(json!({"type":"promote","id":"5","namespace":"demo","service":"batch","instance":"new","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await, json!({"type":"promoted","id":"5"}));
    let waiting = new.recv().await;
    assert_eq!(waiting["type"], "invoke");
    old.send(json!({"type":"chunk","id":active["id"],"output":7}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"chunk","id":"2","output":7})
    );
    new.send(json!({"type":"end","id":waiting["id"]})).await;
    assert_eq!(caller.recv().await["id"], "3");
    caller.call("6", 2000).await;
    let fresh = new.recv().await;
    assert_eq!(fresh["type"], "invoke");
    old.send(json!({"type":"end","id":active["id"]})).await;
    assert_eq!(caller.recv().await, json!({"type":"end","id":"2"}));
    new.send(json!({"type":"end","id":fresh["id"]})).await;
    assert_eq!(caller.recv().await["id"], "6");
    caller.send(json!({"type":"status","id":"7"})).await;
    let status = caller.recv().await;
    let services = status["services"].as_array().unwrap();
    let previous = services.iter().find(|s| s["instance"] == "old").unwrap();
    let selected = services.iter().find(|s| s["instance"] == "new").unwrap();
    assert_eq!(previous["state"], "drained");
    assert_eq!(selected["selected"], true);
    assert_eq!(selected["contract_version"], "1");
    assert_eq!(selected["capabilities"][0]["mode"], "stream");
    let recent = status["recent"].as_array().unwrap();
    assert!(recent
        .iter()
        .all(|r| r["queue_ms"].as_f64().is_some_and(|n| n >= 0.0)));
    assert!(recent
        .iter()
        .all(|r| r["execution_ms"].as_f64().is_some_and(|n| n >= 0.0)));
    server.finish().await;
}

#[tokio::test]
async fn incompatible_missing_and_unready_promotions_leave_old_selection_unchanged() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut old = Host::connect_instance(server.address, "batch", "old").await;
    old.register("unary").await;
    caller.send(json!({"type":"promote","id":"1","namespace":"demo","service":"batch","instance":"old","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    let mut candidate = Host::connect_instance(server.address, "batch", "candidate").await;
    let mut bumped = contract("unary");
    bumped["version"] = json!("2");
    candidate
        .send(json!({"type":"register","max_concurrency":1,"contract":bumped}))
        .await;
    assert_eq!(candidate.recv().await["type"], "registered");
    caller.send(json!({"type":"promote","id":"2","namespace":"demo","service":"batch","instance":"candidate","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["code"], "not_ready");
    candidate.send(json!({"type":"ready"})).await;
    assert_eq!(candidate.recv().await["type"], "ready");
    caller.send(json!({"type":"promote","id":"3","namespace":"demo","service":"batch","instance":"candidate","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["code"], "contract_mismatch");
    caller.send(json!({"type":"promote","id":"4","namespace":"demo","service":"batch","instance":"missing","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["code"], "not_ready");
    let mut bad = Host::connect_instance(server.address, "batch", "bad").await;
    let mut changed = contract("unary");
    changed["methods"]["run"]["output"]["maximum"] = json!(1);
    bad.send(json!({"type":"register","max_concurrency":1,"contract":changed}))
        .await;
    assert_eq!(bad.recv().await["code"], "contract_mismatch");
    caller.call("5", 1000).await;
    let invoke = old.recv().await;
    old.send(json!({"type":"result","id":invoke["id"],"output":9}))
        .await;
    assert_eq!(caller.recv().await["output"], 9);
    caller.send(json!({"type":"status","id":"6"})).await;
    let status = caller.recv().await;
    assert!(status["services"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["instance"] == "old" && s["selected"] == true && s["state"] == "ready"));
    server.finish().await;
}

#[tokio::test]
async fn promotion_drain_deadline_cancels_old_work_and_selected_disconnect_fails_closed() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut old = Host::connect_instance(server.address, "batch", "old").await;
    old.register("stream").await;
    caller.call("1", 2000).await;
    let active = old.recv().await;
    let mut new = Host::connect_instance(server.address, "batch", "new").await;
    new.register("stream").await;
    caller.send(json!({"type":"promote","id":"2","namespace":"demo","service":"batch","instance":"new","timeout_ms":30})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    old.send(json!({"type":"drain","timeout_ms":1000})).await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"error","id":"1","code":"deadline_exceeded","message":"deadline_exceeded"})
    );
    assert_eq!(old.recv().await, json!({"type":"cancel","id":active["id"]}));
    assert_eq!(old.recv().await, json!({"type":"drained"}));
    new.send(json!({"type":"unregister"})).await;
    let mut line = String::new();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), new.read.read_line(&mut line))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    caller.send(json!({"type":"status","id":"3"})).await;
    let status = caller.recv().await;
    assert!(status["services"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["instance"] != "new"));
    assert_eq!(status["recent"][0]["code"], "deadline_exceeded");
    let measured = &status["recent"][0];
    assert!(
        (measured["duration_ms"].as_f64().unwrap()
            - measured["queue_ms"].as_f64().unwrap()
            - measured["execution_ms"].as_f64().unwrap())
        .abs()
            < 1e-9
    );
    let mut replacement = Host::connect_instance(server.address, "batch", "replacement").await;
    replacement.register("stream").await;
    caller.call("20", 1000).await;
    assert_eq!(caller.recv().await["code"], "not_ready");
    caller.send(json!({"type":"promote","id":"21","namespace":"demo","service":"batch","instance":"replacement","timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["type"], "promoted");
    caller.call("22", 1000).await;
    assert_eq!(replacement.recv().await["type"], "invoke");
    server.finish().await;
}

// Prevents cross-connection forgery while proving route/client ID translation.
#[tokio::test]
async fn unary_routing_rejects_forged_responses_and_reports_status() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("unary").await;
    caller.call("1", 1000).await;
    let invocation = callee.recv().await;
    assert_eq!(invocation["caller"]["service"], "front");
    caller.send(json!({"type":"status","id":"2"})).await;
    let status = caller.recv().await;
    assert_eq!(status["calls"][0]["callee_generation"], callee.generation);
    assert!(status["calls"][0].get("input").is_none());
    let mut attacker = Host::connect(server.address, "other").await;
    attacker
        .send(json!({"type":"result","id":invocation["id"],"output":99}))
        .await;
    let mut line = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(1), attacker.read.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    callee
        .send(json!({"type":"result","id":invocation["id"],"output":2}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"result","id":"1","output":2})
    );
    server.finish().await;
}

// Incoming-frame tests do not cover growth from the trusted forwarding envelope.
#[tokio::test]
async fn forwarding_overflow_rejects_only_the_call_and_preserves_the_callee() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, &"f".repeat(256)).await;
    let mut callee = Host::connect(server.address, "batch").await;
    let mut definition = contract("unary");
    definition["methods"]["run"]["input"] = json!({"type":"string"});
    callee
        .send(json!({"type":"register","max_concurrency":1,"contract":definition}))
        .await;
    assert_eq!(callee.recv().await["type"], "registered");
    callee.send(json!({"type":"ready"})).await;
    assert_eq!(callee.recv().await["type"], "ready");
    caller.send(json!({"type":"call","id":"1","namespace":"demo","service":"batch","method":"run","contract_version":"1","input":"small","timeout_ms":1000})).await;
    let active = callee.recv().await;
    let mut large = json!({"type":"call","id":"2","namespace":"demo","service":"batch","method":"run","contract_version":"1","input":"","timeout_ms":1000});
    let payload_bytes = protocol::MAX_FRAME_BYTES - serde_json::to_vec(&large).unwrap().len() - 1;
    large["input"] = json!("x".repeat(payload_bytes));
    assert_eq!(
        serde_json::to_vec(&large).unwrap().len() + 1,
        protocol::MAX_FRAME_BYTES
    );
    caller.send(large).await;
    let rejected = caller.recv().await;
    assert_eq!(
        (rejected["id"].as_str(), rejected["code"].as_str()),
        (Some("2"), Some("invalid_input"))
    );
    callee
        .send(json!({"type":"result","id":active["id"],"output":2}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"result","id":"1","output":2})
    );
    server.finish().await;
}

#[tokio::test]
async fn response_id_expansion_fails_only_its_route_and_keeps_both_connections() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    let mut definition = contract("unary");
    definition["methods"]["run"]["output"] = json!({"type":"string"});
    callee
        .send(json!({"type":"register","max_concurrency":1,"contract":definition}))
        .await;
    assert_eq!(callee.recv().await["type"], "registered");
    callee.send(json!({"type":"ready"})).await;
    assert_eq!(callee.recv().await["type"], "ready");
    caller.call("10000000000000000000", 1000).await;
    let active = callee.recv().await;
    let mut large = json!({"type":"result","id":active["id"],"output":""});
    let payload_bytes = protocol::MAX_FRAME_BYTES - serde_json::to_vec(&large).unwrap().len() - 1;
    large["output"] = json!("x".repeat(payload_bytes));
    callee.send(large).await;
    assert_eq!(caller.recv().await["code"], "invalid_output");
    assert_eq!(callee.recv().await["type"], "cancel");
    caller.call("10000000000000000001", 1000).await;
    let next = callee.recv().await;
    callee
        .send(json!({"type":"result","id":next["id"],"output":"small"}))
        .await;
    assert_eq!(caller.recv().await["output"], "small");
    server.finish().await;
}

// A deadline must include time waiting for registered concurrency, not restart on dispatch.
#[tokio::test]
async fn queued_deadline_expires_without_assignment_and_drain_is_bounded() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("unary").await;
    caller.call("1", 1000).await;
    let first = callee.recv().await;
    caller.call("2", 30).await;
    let expired = caller.recv().await;
    assert_eq!(
        (expired["id"].as_str(), expired["code"].as_str()),
        (Some("2"), Some("deadline_exceeded"))
    );
    callee.send(json!({"type":"drain","timeout_ms":30})).await;
    let cancelled = callee.recv().await;
    assert_eq!(cancelled, json!({"type":"cancel","id":first["id"]}));
    assert_eq!(callee.recv().await["type"], "drained");
    assert_eq!(caller.recv().await["code"], "deadline_exceeded");
    caller.call("3", 1000).await;
    assert_eq!(caller.recv().await["code"], "not_ready");
    server.finish().await;
}

// A stream cannot send before consumer demand, and one credit permits only one chunk.
#[tokio::test]
async fn stream_credit_is_enforced_and_cancellation_retires_routes() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("stream").await;
    caller.call("1", 1000).await;
    let first = callee.recv().await;
    callee
        .send(json!({"type":"chunk","id":first["id"],"output":1}))
        .await;
    assert_eq!(caller.recv().await["code"], "protocol_error");
    assert_eq!(callee.recv().await["type"], "cancel");
    caller.call("2", 1000).await;
    let second = callee.recv().await;
    caller
        .send(json!({"type":"credit","id":"2","credits":1}))
        .await;
    assert_eq!(
        callee.recv().await,
        json!({"type":"credit","id":second["id"],"credits":1})
    );
    callee
        .send(json!({"type":"chunk","id":second["id"],"output":3}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"chunk","id":"2","output":3})
    );
    caller.send(json!({"type":"cancel","id":"2"})).await;
    assert_eq!(callee.recv().await["type"], "cancel");
    assert_eq!(caller.recv().await["code"], "cancelled");
    callee.send(json!({"type":"end","id":second["id"]})).await;
    caller.send(json!({"type":"status","id":"3"})).await;
    assert_eq!(caller.recv().await["calls"], json!([]));
    caller.call("4", 1000).await;
    let third = callee.recv().await;
    caller
        .send(json!({"type":"credit","id":"4","credits":1}))
        .await;
    assert_eq!(callee.recv().await["credits"], 1);
    callee
        .send(json!({"type":"chunk","id":third["id"],"output":4}))
        .await;
    assert_eq!(caller.recv().await["output"], 4);
    callee.send(json!({"type":"end","id":third["id"]})).await;
    assert_eq!(caller.recv().await, json!({"type":"end","id":"4"}));
    caller.call("5", 1000).await;
    let _ = callee.recv().await;
    caller
        .send(json!({"type":"credit","id":"5","credits":17}))
        .await;
    assert_eq!(caller.recv().await["code"], "protocol_error");
    assert_eq!(callee.recv().await["type"], "cancel");
    server.finish().await;
}

// Reconnecting the same identity must not inherit a prior generation's assignment.
#[tokio::test]
async fn disconnect_fails_calls_and_new_generation_cannot_replay() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("unary").await;
    caller.call("1", 1000).await;
    let old = callee.recv().await;
    let old_generation = callee.generation.clone();
    drop(callee);
    assert_eq!(caller.recv().await["code"], "connection_lost");
    let mut callee = Host::connect(server.address, "batch").await;
    assert_ne!(callee.generation, old_generation);
    callee.register("unary").await;
    callee
        .send(json!({"type":"result","id":old["id"],"output":99}))
        .await;
    caller.call("2", 1000).await;
    let new = callee.recv().await;
    assert_ne!(new["id"], old["id"]);
    callee
        .send(json!({"type":"result","id":new["id"],"output":7}))
        .await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"result","id":"2","output":7})
    );
    server.finish().await;
}

// Registration and output enforcement must work across the transport, not only in validator unit tests.
#[tokio::test]
async fn rejects_contract_conflicts_and_invalid_input_output() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("unary").await;
    let mut conflict = Host::connect(server.address, "batch").await;
    conflict
        .send(json!({"type":"register","max_concurrency":1,"contract":contract("stream")}))
        .await;
    assert_eq!(conflict.recv().await["code"], "contract_mismatch");
    caller.send(json!({"type":"call","id":"1","namespace":"demo","service":"batch","method":"run","contract_version":"1","input":null,"timeout_ms":1000})).await;
    assert_eq!(caller.recv().await["code"], "invalid_input");
    caller.call("2", 1000).await;
    let invoke = callee.recv().await;
    callee
        .send(json!({"type":"result","id":invoke["id"],"output":"secret rejected payload"}))
        .await;
    let error = caller.recv().await;
    assert_eq!(error["code"], "invalid_output");
    assert!(!error.to_string().contains("secret"));
    assert_eq!(callee.recv().await["type"], "cancel");
    caller.call("3", 1000).await;
    let invoke = callee.recv().await;
    callee.send(json!({"type":"error","id":invoke["id"],"code":"business","message":"safe business message","details":false})).await;
    assert_eq!(caller.recv().await["code"], "invalid_output");
    assert_eq!(callee.recv().await["type"], "cancel");
    caller.call("4", 1000).await;
    let invoke = callee.recv().await;
    callee.send(json!({"type":"error","id":invoke["id"],"code":"business","message":"safe business message","details":"expected"})).await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"error","id":"4","code":"business","message":"safe business message","details":"expected"})
    );
    caller.call("5", 1000).await;
    let invoke = callee.recv().await;
    callee.send(json!({"type":"error","id":invoke["id"],"code":"cancelled","message":"secret exception","details":{"secret":true}})).await;
    assert_eq!(
        caller.recv().await,
        json!({"type":"error","id":"5","code":"cancelled","message":"cancelled"})
    );
    server.finish().await;
}

// The waiting limit cannot grow with inbound demand while one instance is saturated.
#[tokio::test]
async fn queue_admission_is_bounded_and_resumes_on_available_instance() {
    let server = Server::start().await;
    let mut caller = Host::connect(server.address, "front").await;
    let mut callee = Host::connect(server.address, "batch").await;
    callee.register("unary").await;
    caller.call("1", 5000).await;
    let first = callee.recv().await;
    for id in 2..=129 {
        caller.call(&id.to_string(), 5000).await;
    }
    caller.call("130", 5000).await;
    assert_eq!(caller.recv().await["code"], "overloaded");
    caller.send(json!({"type":"status","id":"131"})).await;
    let status = caller.recv().await;
    let calls = status["calls"].as_array().unwrap();
    assert_eq!(
        calls.iter().filter(|c| c["state"] == "waiting").count(),
        128
    );
    assert_eq!(calls.iter().filter(|c| c["state"] == "active").count(), 1);
    callee
        .send(json!({"type":"result","id":first["id"],"output":1}))
        .await;
    assert_eq!(caller.recv().await["id"], "1");
    let next = callee.recv().await;
    assert_ne!(next["id"], first["id"]);
    server.finish().await;
}

// Duplicate IDs must not attach a second request to either active or retired work.
#[tokio::test]
async fn client_request_ids_are_canonical_monotonic_with_constant_retention() {
    let server = Server::start().await;
    for id in ["0", "01", "18446744073709551616"] {
        let mut host = Host::connect(server.address, "front").await;
        host.send(json!({"type":"status","id":id})).await;
        assert_eq!(host.recv().await["code"], "protocol_error");
    }
    let mut host = Host::connect(server.address, "front").await;
    host.send(json!({"type":"status","id":"9000"})).await;
    assert_eq!(host.recv().await["type"], "status");
    host.send(json!({"type":"status","id":"9000"})).await;
    assert_eq!(host.recv().await["code"], "protocol_error");
    server.finish().await;
}

// Partial oversized frames must close without waiting for a delimiter or allocating without limit.
#[tokio::test]
async fn oversized_and_unknown_frames_close_and_public_bind_is_rejected() {
    assert!(kernel::loopback_address("0.0.0.0:7331").is_err());
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    assert!(kernel::serve(listener, std::future::pending())
        .await
        .is_err());
    let server = Server::start().await;
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    stream
        .write_all(&vec![b'x'; protocol::MAX_FRAME_BYTES])
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let mut host = Host::connect(server.address, "front").await;
    host.send(json!({"type":"unknown"})).await;
    let mut line = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), host.read.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    server.finish().await;
}
