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

fn forwarding_actor(mode: &str) -> (Actor, mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    let mut actor = Actor::default();
    let mut caller = add(&mut actor, 1, "tcp", None);
    let mut service = add(&mut actor, 2, "websocket", None);
    actor.frame(1, hello("caller"));
    actor.frame(2, hello("worker"));
    actor.frame(
        2,
        json!({"type":"register","max_concurrency":1,"contract":{
            "version":"1","methods":{"run":{"mode":mode,"input":{"type":"string"},
            "output":{"type":"string"},"error":{"type":"string"}}}
        }}),
    );
    actor.frame(2, json!({"type":"ready"}));
    while caller.try_recv().is_ok() {}
    while service.try_recv().is_ok() {}
    (actor, caller, service)
}

fn call(id: &str) -> Value {
    json!({"type":"call","id":id,"namespace":"demo","service":"worker",
        "method":"run","contract_version":"1","input":"input","timeout_ms":5000,
        "trace_id":"trace","parent_id":"parent"})
}

fn receive(out: &mut mpsc::Receiver<Vec<u8>>) -> Value {
    serde_json::from_slice(&out.try_recv().unwrap()).unwrap()
}

#[test]
fn prepared_method_handles_are_shared_and_queued_contracts_survive_disconnect() {
    let (mut actor, mut caller, mut service) = forwarding_actor("stream");
    actor.frame(1, call("1"));
    actor.frame(1, call("2"));
    let contract = actor.hosts[&2].contract.as_ref().unwrap().clone();
    let method = contract.method("run").unwrap().clone();
    assert!(Arc::ptr_eq(&actor.calls[&1].definition, &method));
    assert!(Arc::ptr_eq(&actor.calls[&2].definition, &method));
    assert!(Arc::ptr_eq(&actor.calls[&2].contract, &contract));
    while caller.try_recv().is_ok() {}
    while service.try_recv().is_ok() {}
    actor.disconnect(2);
    assert!(!actor.calls.contains_key(&1));
    assert!(actor.calls[&2].callee.is_none());
    assert!(Arc::ptr_eq(&actor.calls[&2].definition, &method));

    let mut replacement = add(&mut actor, 3, "tcp", None);
    let mut identity = hello("worker");
    identity["instance"] = json!("replacement");
    actor.frame(3, identity);
    actor.frame(
        3,
        json!({"type":"register","max_concurrency":1,"contract":contract.original()}),
    );
    actor.frame(3, json!({"type":"ready"}));
    actor.schedule();
    assert_eq!(actor.calls[&2].callee, Some(3));
    assert!(Arc::ptr_eq(&actor.calls[&2].definition, &method));
    while replacement.try_recv().is_ok() {}
    while caller.try_recv().is_ok() {}
    actor.frame(1, json!({"type":"credit","id":"2","credits":1}));
    actor.frame(3, json!({"type":"chunk","id":"2","output":42}));
    assert_eq!(receive(&mut caller)["code"], "invalid_output");
    assert!(!actor.calls.contains_key(&2));
}

fn worker(
    actor: &mut Actor,
    generation: u64,
    namespace: &str,
    service: &str,
    version: &str,
    concurrency: usize,
) -> mpsc::Receiver<Vec<u8>> {
    let mut out = add(actor, generation, "tcp", None);
    actor.frame(
        generation,
        json!({"type":"hello","version":1,"namespace":namespace,
        "service":service,"instance":generation.to_string()}),
    );
    let mut registration = register();
    registration["max_concurrency"] = json!(concurrency);
    registration["contract"]["version"] = json!(version);
    registration["contract"]["methods"]["run"]["input"] = json!({"type":"string"});
    registration["contract"]["methods"]["run"]["output"] = json!({"type":"string"});
    actor.frame(generation, registration);
    actor.frame(generation, json!({"type":"ready"}));
    while out.try_recv().is_ok() {}
    out
}

#[test]
fn scheduling_uses_least_busy_then_generation_and_isolates_scopes() {
    let (mut actor, _caller, _first) = forwarding_actor("stream");
    let _second = worker(&mut actor, 3, "demo", "worker", "1", 2);
    actor.hosts.get_mut(&2).unwrap().concurrency = 2;
    let _namespace = worker(&mut actor, 4, "other", "worker", "1", 2);
    let _service = worker(&mut actor, 5, "demo", "other", "1", 2);
    let _version = worker(&mut actor, 6, "demo", "worker", "2", 2);
    for id in 1..=5 {
        actor.frame(1, call(&id.to_string()));
    }
    assert_eq!(
        actor.calls.values().map(|c| c.callee).collect::<Vec<_>>(),
        vec![Some(2), Some(3), Some(2), Some(3), None]
    );
    actor.frame(1, json!({"type":"cancel","id":"2"}));
    actor.schedule();
    assert_eq!(actor.calls[&5].callee, Some(3));
    actor.disconnect(2);
    actor.frame(1, call("6"));
    assert_eq!(actor.calls[&6].callee, None);
    actor.frame(1, json!({"type":"cancel","id":"4"}));
    actor.schedule();
    assert_eq!(actor.calls[&6].callee, Some(3));
}

#[test]
fn scheduling_global_limit_fifo_and_queued_expiry() {
    let (mut actor, _caller, mut first) = forwarding_actor("stream");
    actor.hosts.get_mut(&2).unwrap().concurrency = ACTIVE;
    let mut second = worker(&mut actor, 3, "demo", "worker", "1", ACTIVE);
    for id in 1..=ACTIVE + WAITING {
        actor.frame(1, call(&id.to_string()));
        while first.try_recv().is_ok() {}
        while second.try_recv().is_ok() {}
    }
    assert_eq!(
        actor.calls.values().filter(|c| c.callee.is_some()).count(),
        ACTIVE
    );
    assert_eq!(
        actor.calls.values().filter(|c| c.callee.is_none()).count(),
        WAITING
    );
    actor.frame(1, call("193"));
    assert_eq!(actor.calls.len(), ACTIVE + WAITING);
    actor.calls.get_mut(&65).unwrap().deadline = Instant::now() - Duration::from_millis(1);
    actor.frame(1, json!({"type":"cancel","id":"1"}));
    actor.schedule();
    assert!(!actor.calls.contains_key(&65));
    assert_eq!(actor.calls[&66].callee, Some(2));
    assert_eq!(actor.calls[&67].callee, None);
}

#[test]
fn promotion_redirects_waiters_and_selected_generation_fails_closed() {
    let (mut actor, _caller, _first) = forwarding_actor("stream");
    actor.frame(1, call("1"));
    actor.frame(1, call("2"));
    let mut second = worker(&mut actor, 3, "demo", "worker", "1", 1);
    actor.promote(
        1,
        &json!({"id":"3","namespace":"demo","service":"worker",
        "instance":"3","timeout_ms":5000}),
    );
    actor.schedule();
    assert_eq!(receive(&mut second)["id"], "2");
    assert_eq!(actor.calls[&1].callee, Some(2));
    actor.disconnect(3);
    assert_eq!(actor.available("demo", "worker", "1"), None);
    actor.frame(1, call("3"));
    assert_eq!(actor.calls.len(), 1);
}

fn original_available(
    actor: &Actor,
    assignments: &BTreeMap<u64, Option<u64>>,
    call: &Call,
    visits: &std::cell::Cell<usize>,
) -> Option<u64> {
    visits.set(visits.get() + assignments.len());
    if assignments.values().filter(|c| c.is_some()).count() >= ACTIVE {
        return None;
    }
    let busy = |host| {
        visits.set(visits.get() + assignments.len());
        assignments.values().filter(|c| **c == Some(host)).count()
    };
    let selected = actor
        .selections
        .get(&(call.namespace.clone(), call.service.clone()));
    actor
        .hosts
        .iter()
        .filter(|(id, h)| {
            selected.is_none_or(|s| s.generation == **id)
                && h.ready
                && h.drain.is_none()
                && h.identity
                    .as_ref()
                    .is_some_and(|i| i.namespace == call.namespace && i.service == call.service)
                && h.contract
                    .as_ref()
                    .is_some_and(|c| c.original()["version"] == call.version)
        })
        .filter(|(id, h)| busy(**id) < h.concurrency)
        .min_by_key(|(id, _)| (busy(**id), **id))
        .map(|(id, _)| *id)
}

#[test]
fn snapshot_matches_original_oracle_and_derives_occupancy_once_at_bounds() {
    let (mut actor, mut caller, mut first) = forwarding_actor("stream");
    let mut receivers = Vec::new();
    for generation in 3..=CONNECTIONS as u64 {
        receivers.push(worker(
            &mut actor,
            generation,
            "demo",
            "worker",
            "1",
            (generation as usize % ACTIVE) + 1,
        ));
    }
    for id in 1..=ACTIVE + WAITING {
        actor.frame(1, call(&id.to_string()));
        while first.try_recv().is_ok() {}
        while caller.try_recv().is_ok() {}
        for out in &mut receivers {
            while out.try_recv().is_ok() {}
        }
    }
    actor.schedule();
    assert_eq!(actor.scheduling_work.call_visits, ACTIVE + WAITING);
    assert_eq!(actor.scheduling_work.host_visits, CONNECTIONS);
    assert_eq!(actor.scheduling_work.candidate_visits, 0);
    for route in 1..=32 {
        actor.fail(route, "cancelled");
    }
    let mut assignments: BTreeMap<_, _> = actor
        .calls
        .iter()
        .map(|(route, c)| (*route, c.callee))
        .collect();
    let visits = std::cell::Cell::new(0);
    for (route, call) in &actor.calls {
        if call.callee.is_none() {
            let callee = original_available(&actor, &assignments, call, &visits);
            assignments.insert(*route, callee);
        }
    }
    let calls = actor.calls.len();
    actor.schedule();
    assert_eq!(
        actor
            .calls
            .iter()
            .map(|(r, c)| (*r, c.callee))
            .collect::<BTreeMap<_, _>>(),
        assignments
    );
    assert_eq!(actor.scheduling_work.call_visits, calls);
    assert_eq!(actor.scheduling_work.host_visits, CONNECTIONS);
    assert_eq!(
        actor.scheduling_work.candidate_visits,
        32 * (CONNECTIONS - 1)
    );
    assert!(
        visits.get()
            > actor.scheduling_work.call_visits
                + actor.scheduling_work.host_visits
                + actor.scheduling_work.candidate_visits
    );
    assert_eq!(
        actor.calls.values().filter(|c| c.callee.is_some()).count(),
        ACTIVE
    );
    for (generation, host) in &actor.hosts {
        assert!(actor.busy(*generation) <= host.concurrency);
    }
}

#[test]
fn snapshot_keeps_capacity_through_waiting_failures_and_writer_retirement() {
    let (mut actor, mut caller, mut service) = forwarding_actor("stream");
    actor.frame(1, call("1"));
    receive(&mut service);
    for id in 2..=5 {
        actor.frame(1, call(&id.to_string()));
    }
    actor.calls.get_mut(&2).unwrap().deadline = Instant::now() - Duration::from_millis(1);
    actor.calls.get_mut(&3).unwrap().input = json!("x".repeat(MAX_FRAME_BYTES));
    actor.fail(1, "cancelled");
    receive(&mut service);
    actor.schedule();
    assert!(!actor.calls.contains_key(&2));
    assert!(!actor.calls.contains_key(&3));
    assert_eq!(actor.calls[&4].callee, Some(2));
    assert_eq!(actor.calls[&5].callee, None);
    assert_eq!(actor.scheduling_work.call_visits, 4);
    receive(&mut service);
    actor.fail(4, "cancelled");
    receive(&mut service);
    while caller.try_recv().is_ok() {}
    drop(service);
    actor.schedule();
    assert_eq!(actor.calls[&5].callee, Some(2));
    assert!(actor.failed_writers.contains(&2));
    actor.maintenance();
    assert!(!actor.hosts.contains_key(&2));
    assert!(actor.calls.is_empty());
    assert_eq!(actor.scheduling_work.call_visits, 0);
}

#[test]
fn derived_control_lookup_scopes_ids_and_enforces_credit_sixteen() {
    let (mut actor, _caller, mut service) = forwarding_actor("stream");
    let _other = add(&mut actor, 3, "tcp", None);
    actor.frame(3, hello("other"));
    actor.frame(1, call("1"));
    actor.frame(3, call("1"));
    actor.frame(3, json!({"type":"credit","id":"1","credits":16}));
    assert_eq!(actor.calls[&1].credits, 0);
    assert_eq!(actor.calls[&2].credits, 16);
    actor.frame(3, json!({"type":"credit","id":"1","credits":1}));
    assert!(!actor.calls.contains_key(&2));
    actor.frame(3, json!({"type":"cancel","id":"1"}));
    assert!(actor.calls.contains_key(&1));
    receive(&mut service);
    actor.frame(1, json!({"type":"cancel","id":"1"}));
    actor.frame(1, json!({"type":"credit","id":"1","credits":16}));
    assert!(actor.calls.is_empty());
}

#[test]
fn forwarding_rewrites_results_chunks_and_business_errors_and_records_terminals() {
    for (mode, response) in [
        ("unary", json!({"type":"result","id":"1","output":"result"})),
        ("stream", json!({"type":"chunk","id":"1","output":"chunk"})),
        (
            "unary",
            json!({"type":"error","id":"1","code":"business",
            "message":"business message","details":"business details"}),
        ),
        (
            "unary",
            json!({"type":"error","id":"1","code":"connection_lost",
            "message":"private exception","details":{"private":"payload"}}),
        ),
    ] {
        let (mut actor, mut caller, mut service) = forwarding_actor(mode);
        actor.frame(1, call("10000000000000000000"));
        let invoke = receive(&mut service);
        assert_eq!(
            invoke["caller"],
            json!({"namespace":"demo","service":"caller","instance":"one"})
        );
        assert_eq!(invoke["trace_id"], "trace");
        assert_eq!(invoke["parent_id"], "parent");
        if mode == "stream" {
            actor.frame(
                1,
                json!({"type":"credit","id":"10000000000000000000","credits":1}),
            );
            assert_eq!(receive(&mut service)["type"], "credit");
        }
        let mut expected = response.clone();
        expected["id"] = json!("10000000000000000000");
        if response["code"] == "connection_lost" {
            expected = json!({"type":"error","id":"10000000000000000000",
                "code":"connection_lost","message":"connection_lost"});
        }
        actor.frame(2, response);
        assert_eq!(receive(&mut caller), expected);
        if mode == "stream" {
            assert_eq!(actor.calls[&1].credits, 0);
            assert!(actor.recent.is_empty());
            actor.frame(2, json!({"type":"end","id":"1"}));
            assert_eq!(
                receive(&mut caller),
                json!({"type":"end","id":"10000000000000000000"})
            );
        }
        assert!(actor.calls.is_empty());
        assert_eq!(actor.recent.len(), 1);
        assert_eq!(
            actor.recent[0]["outcome"],
            if mode == "stream" {
                json!("end")
            } else {
                expected["type"].clone()
            }
        );
        assert_eq!(
            actor.recent[0]["code"],
            expected.get("code").unwrap_or(&Value::Null).clone()
        );
        actor.frame(2, json!({"type":"result","id":"1","output":"late"}));
        assert!(caller.try_recv().is_err());
        assert!(actor.failed_writers.is_empty());
    }
}

#[test]
fn queued_invocation_uses_remaining_deadline_and_does_not_assign_expired_work() {
    let (mut actor, mut caller, mut service) = forwarding_actor("unary");
    actor.frame(1, call("1"));
    receive(&mut service);
    actor.frame(1, call("2"));
    actor.frame(1, call("3"));
    assert_eq!(actor.calls[&2].callee, None);
    actor.calls.get_mut(&2).unwrap().deadline = Instant::now() + Duration::from_millis(250);
    actor.calls.get_mut(&3).unwrap().deadline = Instant::now() - Duration::from_millis(1);
    actor.frame(2, json!({"type":"result","id":"1","output":"done"}));
    actor.schedule();
    let invoke = receive(&mut service);
    assert_eq!(invoke["id"], "2");
    assert!(invoke["timeout_ms"]
        .as_u64()
        .is_some_and(|ms| ms > 0 && ms <= 250));
    assert_eq!(receive(&mut caller)["type"], "result");
    assert_eq!(receive(&mut caller)["code"], "deadline_exceeded");
    assert!(!actor.calls.contains_key(&3));
    assert!(service.try_recv().is_err());
}

#[test]
fn encoder_counts_newline_and_enqueue_moves_the_once_encoded_buffer() {
    struct Counted<'a>(&'a std::cell::Cell<usize>, &'a str);
    impl Serialize for Counted<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.set(self.0.get() + 1);
            serializer.serialize_str(self.1)
        }
    }
    let count = std::cell::Cell::new(0);
    let payload = "x".repeat(MAX_FRAME_BYTES - 3);
    let encoded = encode(&Counted(&count, &payload)).unwrap();
    assert_eq!(encoded.0.len(), MAX_FRAME_BYTES);
    assert_eq!(encoded.0.last(), Some(&b'\n'));
    let allocation = encoded.0.as_ptr();
    let mut actor = Actor::default();
    let mut out = add(&mut actor, 1, "tcp", None);
    actor.enqueue(1, encoded);
    let received = out.try_recv().unwrap();
    assert_eq!(received.as_ptr(), allocation);
    assert_eq!(count.get(), 1);
    assert!(encode(&"x".repeat(MAX_FRAME_BYTES - 2)).is_none());
}

#[test]
fn admission_moves_input_and_invocation_borrows_the_retained_payload() {
    let (mut actor, _caller, mut service) = forwarding_actor("unary");
    actor.frame(1, call("1"));
    receive(&mut service);
    let mut queued = call("2");
    queued["input"] = json!("retained".repeat(1024));
    let allocation = queued["input"].as_str().unwrap().as_ptr();
    actor.frame(1, queued);
    let retained = &actor.calls[&2].input;
    assert_eq!(retained.as_str().unwrap().as_ptr(), allocation);
    let invocation = actor.invocation(2, &actor.calls[&2]).unwrap();
    assert!(std::ptr::eq(invocation.input, retained));
    let serialized = serde_json::from_slice::<Value>(&encode(&invocation).unwrap().0).unwrap();
    assert_eq!(serialized["input"], *retained);
    assert_eq!(serialized["parent_id"], "parent");
    assert_eq!(serialized["caller"]["service"], "caller");
}
