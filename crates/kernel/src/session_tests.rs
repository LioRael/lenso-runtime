use super::*;

fn add(
    actor: &mut Actor,
    id: u64,
    kind: &'static str,
    expected: Option<Identity>,
) -> mpsc::Receiver<Vec<u8>> {
    let (out, receiver) = mpsc::channel(OUTBOUND);
    let (close, _) = watch::channel(());
    actor.hosts.insert(
        id,
        Host {
            out,
            _close: close,
            transport: kind,
            expected,
            identity: None,
            contract: None,
            concurrency: 0,
            ready: false,
            drain: None,
            drained: false,
            notify_drained: false,
            seen: Instant::now(),
            last_id: 0,
        },
    );
    receiver
}

fn key() -> (String, String, String) {
    ("demo".into(), "worker".into(), "one".into())
}

fn hello(service: &str) -> Value {
    json!({"type":"hello","version":1,"namespace":"demo","service":service,"instance":"one"})
}

fn register() -> Value {
    json!({"type":"register","max_concurrency":1,"contract":{"version":"1","methods":{
        "run":{"mode":"stream","input":{"type":"integer"},"output":{"type":"integer"},"error":{"type":"string"}}
    }}})
}

#[test]
fn stdio_expected_identity_is_checked_before_welcome_or_registration() {
    let mut actor = Actor::default();
    let mut out = add(
        &mut actor,
        1,
        "stdio",
        Some(Identity {
            namespace: "demo".into(),
            service: "worker".into(),
            instance: "one".into(),
        }),
    );
    actor.frame(1, hello("wrong"));
    actor.frame(1, register());
    actor.frame(1, json!({"type":"ready"}));
    assert!(actor.hosts[&1].identity.is_none());
    assert!(actor.hosts[&1].contract.is_none());
    assert_eq!(
        serde_json::from_slice::<Value>(&out.try_recv().unwrap()).unwrap()["code"],
        "protocol_error"
    );
}

#[test]
fn managed_identity_is_exclusive_and_ready_transition_survives_immediate_exit() {
    let mut actor = Actor::default();
    actor.managed_identities.insert(
        key(),
        Reservation {
            generation: None,
            transport: "tcp",
            ready: false,
        },
    );
    let _owner = add(&mut actor, 1, "tcp", None);
    let mut duplicate = add(&mut actor, 2, "tcp", None);
    let mut wrong_transport = add(&mut actor, 3, "websocket", None);
    actor.frame(3, hello("worker"));
    assert_eq!(
        serde_json::from_slice::<Value>(&wrong_transport.try_recv().unwrap()).unwrap()["code"],
        "protocol_error"
    );
    actor.frame(1, hello("worker"));
    actor.frame(1, register());
    actor.frame(1, json!({"type":"ready"}));
    actor.frame(1, json!({"type":"unregister"}));
    assert!(!actor.hosts.contains_key(&1));
    assert!(actor.managed_identities[&key()].ready);
    actor.frame(2, hello("worker"));
    assert_eq!(
        serde_json::from_slice::<Value>(&duplicate.try_recv().unwrap()).unwrap()["code"],
        "protocol_error"
    );
    assert_eq!(actor.managed_identities[&key()].generation, Some(1));
}

#[test]
fn shutdown_rejects_new_calls_and_never_assigns_waiting_independent_work() {
    let mut actor = Actor::default();
    let mut caller = add(&mut actor, 1, "tcp", None);
    let mut service = add(&mut actor, 2, "websocket", None);
    actor.frame(1, hello("caller"));
    actor.frame(2, hello("worker"));
    actor.frame(2, register());
    actor.frame(2, json!({"type":"ready"}));
    for id in ["1", "2"] {
        actor.frame(
            1,
            json!({"type":"call","id":id,"namespace":"demo","service":"worker",
            "method":"run","contract_version":"1","input":1,"timeout_ms":5000}),
        );
    }
    assert_eq!(actor.calls[&1].callee, Some(2));
    assert_eq!(actor.calls[&2].callee, None);
    while service.try_recv().is_ok() {}
    while caller.try_recv().is_ok() {}
    actor.shutting_down = true;
    actor.frame(2, json!({"type":"end","id":"1"}));
    actor.maintenance();
    assert_eq!(actor.calls[&2].callee, None);
    assert!(service.try_recv().is_err());
    actor.frame(
        1,
        json!({"type":"call","id":"3","namespace":"demo","service":"worker",
        "method":"run","contract_version":"1","input":1,"timeout_ms":5000}),
    );
    let _end = caller.try_recv().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&caller.try_recv().unwrap()).unwrap()["code"],
        "not_ready"
    );
    assert_eq!(actor.hosts[&1].last_id, 3);
}
