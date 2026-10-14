//! Bounded loopback routing kernel. Only the routing actor mutates lifecycle and calls.
use protocol::{
    json_equivalent, validate_contract, validate_host_frame, validate_value, MAX_FRAME_BYTES,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant};

pub use transport::diagnostics;
pub mod process;
#[cfg(test)]
mod session_tests;

const CONNECTIONS: usize = 128;
const ACTIVE: usize = 64;
const WAITING: usize = 128;
const OUTBOUND: usize = 64;
const EVENTS: usize = 256;
const LIVENESS: Duration = Duration::from_secs(5);

enum Event {
    Frame(u64, Value),
    Gone(u64),
}

#[derive(Clone)]
struct Identity {
    namespace: String,
    service: String,
    instance: String,
}

struct Host {
    out: mpsc::Sender<Vec<u8>>,
    _close: watch::Sender<()>,
    transport: &'static str,
    expected: Option<Identity>,
    identity: Option<Identity>,
    contract: Option<Arc<Value>>,
    concurrency: usize,
    ready: bool,
    drain: Option<Instant>,
    drained: bool,
    notify_drained: bool,
    seen: Instant,
    last_id: u64,
}

struct Selection {
    generation: u64,
    contract: Arc<Value>,
}

struct Reservation {
    generation: Option<u64>,
    transport: &'static str,
    ready: bool,
}

struct Call {
    caller: u64,
    client_id: String,
    namespace: String,
    service: String,
    version: String,
    method: String,
    contract: Arc<Value>,
    definition: Value,
    input: Value,
    trace: String,
    parent: Option<String>,
    started: Instant,
    assigned: Option<Instant>,
    deadline: Instant,
    callee: Option<u64>,
    credits: u64,
}

#[derive(Default)]
struct Actor {
    hosts: BTreeMap<u64, Host>,
    calls: BTreeMap<u64, Call>,
    recent: VecDeque<Value>,
    failed_writers: HashSet<u64>,
    next_route: u64,
    selections: BTreeMap<(String, String), Selection>,
    managed_identities: BTreeMap<(String, String, String), Reservation>,
    shutting_down: bool,
}

fn string(frame: &Value, key: &str) -> String {
    frame[key].as_str().unwrap_or("").to_owned()
}

impl Actor {
    fn send(&mut self, host: u64, frame: Value) {
        let Ok(mut bytes) = serde_json::to_vec(&frame) else {
            self.failed_writers.insert(host);
            return;
        };
        bytes.push(b'\n');
        if bytes.len() > MAX_FRAME_BYTES {
            self.error(host, frame.get("id").and_then(Value::as_str), "overloaded");
            return;
        }
        if self
            .hosts
            .get(&host)
            .is_some_and(|h| h.out.try_send(bytes).is_err())
        {
            self.failed_writers.insert(host);
        }
    }

    fn error(&mut self, host: u64, id: Option<&str>, code: &str) {
        let mut frame = json!({"type":"error","code":code,"message":code});
        if let Some(id) = id {
            frame["id"] = json!(id);
        }
        self.send(host, frame);
    }

    fn protocol_error(&mut self, host: u64) {
        self.error(host, None, "protocol_error");
        self.failed_writers.insert(host);
    }

    fn busy(&self, host: u64) -> usize {
        self.calls
            .values()
            .filter(|c| c.callee == Some(host))
            .count()
    }

    fn available(&self, namespace: &str, service: &str, version: &str) -> Option<u64> {
        if self.calls.values().filter(|c| c.callee.is_some()).count() >= ACTIVE {
            return None;
        }
        let selected = self
            .selections
            .get(&(namespace.to_owned(), service.to_owned()));
        self.hosts
            .iter()
            .filter(|(id, h)| {
                selected.is_none_or(|s| s.generation == **id)
                    && h.ready
                    && h.drain.is_none()
                    && h.identity
                        .as_ref()
                        .is_some_and(|i| i.namespace == namespace && i.service == service)
                    && h.contract.as_ref().is_some_and(|c| c["version"] == version)
            })
            .filter(|(id, h)| self.busy(**id) < h.concurrency)
            .min_by_key(|(id, _)| (self.busy(**id), **id))
            .map(|(id, _)| *id)
    }

    fn request_id(&mut self, host: u64, frame: &Value) -> bool {
        let id = frame["id"].as_str().unwrap_or("");
        let parsed = id
            .parse::<u64>()
            .ok()
            .filter(|number| number.to_string() == id);
        let Some(number) = parsed else {
            self.protocol_error(host);
            return false;
        };
        if number <= self.hosts[&host].last_id {
            self.protocol_error(host);
            return false;
        }
        if let Some(h) = self.hosts.get_mut(&host) {
            h.last_id = number;
        }
        true
    }

    fn terminal(&mut self, route: u64, frame: Value, cancel: bool) {
        if let Some(call) = self.calls.remove(&route) {
            if self.recent.len() == 64 {
                self.recent.pop_front();
            }
            let ended = Instant::now();
            self.recent.push_back(json!({
                "id":route.to_string(),"namespace":call.namespace,"service":call.service,
                "method":call.method,"trace_id":call.trace,"parent_id":call.parent,
                "outcome":frame["type"],"code":frame.get("code"),
                "duration_ms":ended.duration_since(call.started).as_secs_f64() * 1000.0,
                "queue_ms":call.assigned.unwrap_or(ended).duration_since(call.started).as_secs_f64() * 1000.0,
                "execution_ms":call.assigned.map(|assigned| ended.duration_since(assigned).as_secs_f64() * 1000.0)
            }));
            if cancel {
                if let Some(callee) = call.callee {
                    self.send(callee, json!({"type":"cancel","id":route.to_string()}));
                }
            }
            let mut frame = frame;
            frame["id"] = json!(call.client_id);
            self.send(call.caller, frame);
        }
    }

    fn fail(&mut self, route: u64, code: &str) {
        self.terminal(
            route,
            json!({"type":"error","code":code,"message":code}),
            true,
        );
    }

    fn disconnect(&mut self, host: u64) {
        self.hosts.remove(&host);
        let routes: Vec<_> = self
            .calls
            .iter()
            .filter(|(_, c)| c.caller == host || c.callee == Some(host))
            .map(|(id, _)| *id)
            .collect();
        for route in routes {
            self.fail(route, "connection_lost");
        }
    }

    fn maintenance(&mut self) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .hosts
            .iter()
            .filter(|(_, h)| now.duration_since(h.seen) >= LIVENESS)
            .map(|(id, _)| *id)
            .collect();
        for host in expired {
            self.disconnect(host);
        }
        let deadlines: Vec<_> = self
            .calls
            .iter()
            .filter(|(_, c)| c.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for route in deadlines {
            self.fail(route, "deadline_exceeded");
        }
        let draining: Vec<_> = self
            .hosts
            .iter()
            .filter(|(_, h)| h.drain.is_some() && !h.drained)
            .map(|(id, h)| (*id, h.drain))
            .collect();
        for (host, deadline) in draining {
            let routes: Vec<_> = self
                .calls
                .iter()
                .filter(|(_, c)| {
                    c.callee == Some(host) || (self.hosts[&host].notify_drained && c.caller == host)
                })
                .map(|(id, _)| *id)
                .collect();
            if deadline.is_some_and(|d| d <= now) {
                for route in &routes {
                    self.fail(*route, "deadline_exceeded");
                }
            }
            if routes.is_empty() || deadline.is_some_and(|d| d <= now) {
                if let Some(h) = self.hosts.get_mut(&host) {
                    h.drained = true;
                }
                if self.hosts[&host].notify_drained {
                    self.send(host, json!({"type":"drained"}));
                }
            }
        }
        while let Some(host) = self.failed_writers.iter().next().copied() {
            self.failed_writers.remove(&host);
            self.disconnect(host);
        }
        self.schedule();
    }

    fn schedule(&mut self) {
        if self.shutting_down {
            return;
        }
        let waiting: Vec<_> = self
            .calls
            .iter()
            .filter(|(_, c)| c.callee.is_none())
            .map(|(id, _)| *id)
            .collect();
        for route in waiting {
            if self.calls.values().filter(|c| c.callee.is_some()).count() >= ACTIVE {
                break;
            }
            if self
                .calls
                .get(&route)
                .is_some_and(|call| call.deadline <= Instant::now())
            {
                self.fail(route, "deadline_exceeded");
                continue;
            }
            let Some(call) = self.calls.get(&route) else {
                continue;
            };
            let callee = self.available(&call.namespace, &call.service, &call.version);
            if let Some(callee) = callee {
                let Some(invoke) = self.invocation(route, call) else {
                    continue;
                };
                let credits = call.credits;
                if let Some(call) = self.calls.get_mut(&route) {
                    call.callee = Some(callee);
                    call.assigned = Some(Instant::now());
                }
                self.send(callee, invoke);
                if credits > 0 {
                    self.send(
                        callee,
                        json!({"type":"credit","id":route.to_string(),"credits":credits}),
                    );
                }
            }
        }
    }

    fn invocation(&self, route: u64, call: &Call) -> Option<Value> {
        let identity = self.hosts.get(&call.caller)?.identity.as_ref()?;
        let mut invoke = json!({
            "type":"invoke","id":route.to_string(),"method":call.method,"input":call.input,
            "timeout_ms":call.deadline.saturating_duration_since(Instant::now()).as_millis().max(1),
            "trace_id":call.trace,
            "caller":{"namespace":identity.namespace,"service":identity.service,"instance":identity.instance}
        });
        if let Some(parent) = &call.parent {
            invoke["parent_id"] = json!(parent);
        }
        Some(invoke)
    }

    fn frame(&mut self, host: u64, frame: Value) {
        if !self.hosts.contains_key(&host) {
            return;
        }
        if validate_host_frame(&frame).is_err() {
            self.protocol_error(host);
            return;
        }
        let kind = frame["type"].as_str().unwrap_or("");
        let greeted = self.hosts[&host].identity.is_some();
        if self.shutting_down && matches!(kind, "hello" | "register" | "ready" | "call" | "promote")
        {
            if !greeted && kind != "hello" {
                self.protocol_error(host);
                return;
            }
            if matches!(kind, "call" | "promote") && !self.request_id(host, &frame) {
                return;
            }
            self.error(host, frame.get("id").and_then(Value::as_str), "not_ready");
            return;
        }
        if !greeted {
            if kind != "hello" {
                self.protocol_error(host);
                return;
            }
            let identity = Identity {
                namespace: string(&frame, "namespace"),
                service: string(&frame, "service"),
                instance: string(&frame, "instance"),
            };
            if self.hosts[&host].expected.as_ref().is_some_and(|expected| {
                expected.namespace != identity.namespace
                    || expected.service != identity.service
                    || expected.instance != identity.instance
            }) {
                self.protocol_error(host);
                return;
            }
            let key = (
                identity.namespace.clone(),
                identity.service.clone(),
                identity.instance.clone(),
            );
            if let Some(reservation) = self.managed_identities.get_mut(&key) {
                if reservation.transport != self.hosts[&host].transport
                    || reservation.generation.is_some_and(|owner| owner != host)
                {
                    self.protocol_error(host);
                    return;
                }
                reservation.generation = Some(host);
            }
            if let Some(h) = self.hosts.get_mut(&host) {
                h.identity = Some(identity);
                h.seen = Instant::now();
            }
            self.send(host, json!({"type":"welcome","version":1,"generation":host.to_string(),"heartbeat_ms":1000}));
            return;
        }
        match kind {
            "heartbeat" => {
                if let Some(h) = self.hosts.get_mut(&host) {
                    h.seen = Instant::now();
                }
            }
            "hello" => self.protocol_error(host),
            "register" => self.register(host, &frame),
            "ready" => {
                let h = &self.hosts[&host];
                if h.contract.is_none() || h.ready || h.drain.is_some() {
                    self.protocol_error(host);
                    return;
                }
                if let Some(h) = self.hosts.get_mut(&host) {
                    h.ready = true;
                }
                if let Some(reservation) = self
                    .managed_identities
                    .values_mut()
                    .find(|reservation| reservation.generation == Some(host))
                {
                    reservation.ready = true;
                }
                self.send(host, json!({"type":"ready"}));
            }
            "call" => self.admit(host, frame),
            "result" | "chunk" | "end" | "error" => self.response(host, frame),
            "credit" | "cancel" => self.control(host, frame),
            "promote" => {
                if self.request_id(host, &frame) {
                    self.promote(host, &frame);
                }
            }
            "drain" => {
                if self.hosts[&host].notify_drained {
                    self.protocol_error(host);
                    return;
                }
                if let Some(h) = self.hosts.get_mut(&host) {
                    h.ready = false;
                    h.notify_drained = true;
                    h.drained = false;
                    let deadline = Instant::now()
                        + Duration::from_millis(
                            frame["timeout_ms"].as_u64().unwrap_or(0).min(60_000),
                        );
                    h.drain = Some(h.drain.map_or(deadline, |old| old.min(deadline)));
                }
            }
            "unregister" => self.disconnect(host),
            "status" => {
                if self.request_id(host, &frame) {
                    self.status(host, &frame);
                }
            }
            _ => self.protocol_error(host),
        }
    }

    fn promote(&mut self, operator: u64, frame: &Value) {
        let id = string(frame, "id");
        let namespace = string(frame, "namespace");
        let service = string(frame, "service");
        let instance = string(frame, "instance");
        let key = (namespace.clone(), service.clone());
        let milliseconds = frame["timeout_ms"].as_u64().unwrap_or(0);
        if !(1..=60_000).contains(&milliseconds) {
            self.error(operator, Some(&id), "invalid_input");
            return;
        }
        let candidates: Vec<_> = self
            .hosts
            .iter()
            .filter(|(_, h)| {
                h.identity.as_ref().is_some_and(|i| {
                    i.namespace == namespace && i.service == service && i.instance == instance
                }) && h.ready
                    && h.drain.is_none()
                    && h.contract.is_some()
            })
            .map(|(generation, _)| *generation)
            .collect();
        if candidates.len() != 1 {
            self.error(operator, Some(&id), "not_ready");
            return;
        }
        let candidate = candidates[0];
        let Some(contract) = self.hosts[&candidate].contract.as_ref().map(Arc::clone) else {
            return;
        };
        let incompatible = self.hosts.values().any(|h| {
            h.ready
                && h.drain.is_none()
                && h.identity
                    .as_ref()
                    .is_some_and(|i| i.namespace == namespace && i.service == service)
                && h.contract
                    .as_ref()
                    .is_some_and(|c| !json_equivalent(c, &contract))
        }) || self.calls.values().any(|c| {
            c.namespace == namespace
                && c.service == service
                && !json_equivalent(&c.contract, &contract)
        }) || self
            .selections
            .get(&key)
            .is_some_and(|s| !json_equivalent(&s.contract, &contract));
        if incompatible {
            self.error(operator, Some(&id), "contract_mismatch");
            return;
        }
        if !self.selections.contains_key(&key) && self.selections.len() >= CONNECTIONS {
            self.error(operator, Some(&id), "overloaded");
            return;
        }
        // The actor validates and commits without yielding; unassigned calls follow the selection.
        self.selections.insert(
            key,
            Selection {
                generation: candidate,
                contract,
            },
        );
        let deadline = Instant::now() + Duration::from_millis(milliseconds);
        for (generation, h) in &mut self.hosts {
            if *generation != candidate
                && h.identity
                    .as_ref()
                    .is_some_and(|i| i.namespace == namespace && i.service == service)
            {
                h.ready = false;
                h.drain = Some(h.drain.map_or(deadline, |old| old.min(deadline)));
            }
        }
        self.send(operator, json!({"type":"promoted","id":id}));
    }

    fn register(&mut self, host: u64, frame: &Value) {
        let h = &self.hosts[&host];
        let concurrency = frame["max_concurrency"].as_u64().unwrap_or(0);
        if h.contract.is_some()
            || h.drain.is_some()
            || !(1..=64).contains(&concurrency)
            || validate_contract(&frame["contract"]).is_err()
        {
            self.protocol_error(host);
            return;
        }
        let Some(identity) = &h.identity else {
            return;
        };
        let conflict = self.hosts.iter().any(|(id, other)| {
            *id != host
                && other.identity.as_ref().is_some_and(|i| {
                    i.namespace == identity.namespace && i.service == identity.service
                })
                && other.contract.as_ref().is_some_and(|c| {
                    c["version"] == frame["contract"]["version"]
                        && !json_equivalent(c, &frame["contract"])
                })
        });
        // Queued calls retain the admitted contract even after all old instances leave.
        let conflict = conflict
            || self.calls.values().any(|c| {
                c.namespace == identity.namespace
                    && c.service == identity.service
                    && c.version == frame["contract"]["version"]
                    && !json_equivalent(&c.contract, &frame["contract"])
            })
            || self
                .selections
                .get(&(identity.namespace.clone(), identity.service.clone()))
                .is_some_and(|s| {
                    s.contract["version"] == frame["contract"]["version"]
                        && !json_equivalent(&s.contract, &frame["contract"])
                });
        if conflict {
            self.error(host, None, "contract_mismatch");
            self.failed_writers.insert(host);
            return;
        }
        if let Some(h) = self.hosts.get_mut(&host) {
            h.contract = Some(Arc::new(frame["contract"].clone()));
            h.concurrency = concurrency as usize;
        }
        self.send(host, json!({"type":"registered"}));
    }

    fn admit(&mut self, host: u64, frame: Value) {
        let id = string(&frame, "id");
        if !self.request_id(host, &frame) {
            return;
        }
        if self.hosts[&host].drain.is_some() {
            self.error(host, Some(&id), "not_ready");
            return;
        }
        let namespace = string(&frame, "namespace");
        let service = string(&frame, "service");
        let version = string(&frame, "contract_version");
        let method = string(&frame, "method");
        let matching: Vec<_> = self
            .hosts
            .values()
            .filter(|h| {
                h.identity
                    .as_ref()
                    .is_some_and(|i| i.namespace == namespace && i.service == service)
            })
            .collect();
        let contract = matching
            .iter()
            .find_map(|h| h.contract.as_ref().filter(|c| c["version"] == version));
        let Some(contract) = contract else {
            let code = if matching.is_empty() {
                "service_not_found"
            } else if matching.iter().all(|h| h.contract.is_none()) {
                "not_ready"
            } else {
                "contract_mismatch"
            };
            self.error(host, Some(&id), code);
            return;
        };
        let contract = Arc::clone(contract);
        let definition = contract["methods"][&method].clone();
        if definition.is_null() {
            self.error(host, Some(&id), "contract_mismatch");
            return;
        }
        let selected = self.selections.get(&(namespace.clone(), service.clone()));
        if !self.hosts.iter().any(|(generation, h)| {
            selected.is_none_or(|s| s.generation == *generation)
                && h.identity
                    .as_ref()
                    .is_some_and(|i| i.namespace == namespace && i.service == service)
                && h.ready
                && h.drain.is_none()
                && h.contract.as_ref().is_some_and(|c| c["version"] == version)
        }) {
            self.error(host, Some(&id), "not_ready");
            return;
        }
        if validate_value(&definition["input"], &frame["input"]).is_err() {
            self.error(host, Some(&id), "invalid_input");
            return;
        }
        let milliseconds = frame["timeout_ms"].as_u64().unwrap_or(0).min(60_000);
        if milliseconds == 0 {
            self.error(host, Some(&id), "deadline_exceeded");
            return;
        }
        self.schedule();
        if self.calls.values().filter(|c| c.callee.is_none()).count() >= WAITING
            && self.available(&namespace, &service, &version).is_none()
        {
            self.error(host, Some(&id), "overloaded");
            return;
        }
        let Some(route) = self.next_route.checked_add(1) else {
            self.protocol_error(host);
            return;
        };
        self.next_route = route;
        let call = Call {
            caller: host,
            client_id: id,
            namespace,
            service,
            version,
            method,
            contract,
            definition,
            input: frame["input"].clone(),
            trace: frame
                .get("trace_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| route.to_string()),
            parent: frame
                .get("parent_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            started: Instant::now(),
            assigned: None,
            deadline: Instant::now() + Duration::from_millis(milliseconds),
            callee: None,
            credits: 0,
        };
        let fits = self
            .invocation(route, &call)
            .and_then(|invoke| serde_json::to_vec(&invoke).ok())
            .is_some_and(|bytes| bytes.len() < MAX_FRAME_BYTES);
        if !fits {
            self.error(host, Some(&call.client_id), "invalid_input");
            return;
        }
        self.calls.insert(route, call);
        self.schedule();
    }

    fn response(&mut self, host: u64, frame: Value) {
        let id = string(&frame, "id");
        let route = id.parse::<u64>().ok().filter(|r| r.to_string() == id);
        let Some(route) = route else {
            self.protocol_error(host);
            return;
        };
        let Some(call) = self.calls.get(&route) else {
            // Late cooperative cancellation completions cannot revive a retired route.
            return;
        };
        if call.callee != Some(host) {
            self.protocol_error(host);
            return;
        }
        if call.deadline <= Instant::now() {
            self.fail(route, "deadline_exceeded");
            return;
        }
        let kind = frame["type"].as_str().unwrap_or("");
        let stream = call.definition["mode"] == "stream";
        let valid = match kind {
            "result" => {
                !stream && validate_value(&call.definition["output"], &frame["output"]).is_ok()
            }
            "chunk" => {
                stream
                    && call.credits > 0
                    && validate_value(&call.definition["output"], &frame["output"]).is_ok()
            }
            "end" => stream,
            "error" => {
                frame["code"] != "business"
                    || validate_value(
                        &call.definition["error"],
                        frame.get("details").unwrap_or(&Value::Null),
                    )
                    .is_ok()
            }
            _ => false,
        };
        if !valid {
            let code = if kind == "chunk" && stream && call.credits == 0 {
                "protocol_error"
            } else {
                "invalid_output"
            };
            self.fail(route, code);
            return;
        }
        let mut forwarded = frame.clone();
        forwarded["id"] = json!(call.client_id);
        if serde_json::to_vec(&forwarded).map_or(true, |bytes| bytes.len() >= MAX_FRAME_BYTES) {
            self.fail(route, "invalid_output");
            return;
        }
        if kind == "chunk" {
            let caller = call.caller;
            let client_id = call.client_id.clone();
            if let Some(call) = self.calls.get_mut(&route) {
                call.credits -= 1;
            }
            let mut frame = frame;
            frame["id"] = json!(client_id);
            self.send(caller, frame);
        } else {
            // Infrastructure diagnostics cannot leak host exception text or payload details.
            let safe = if kind == "error" && frame["code"] != "business" {
                json!({"type":"error","code":frame["code"],"message":frame["code"]})
            } else {
                frame
            };
            self.terminal(route, safe, false);
        }
    }

    fn control(&mut self, host: u64, frame: Value) {
        let id = string(&frame, "id");
        let route = self
            .calls
            .iter()
            .find(|(_, c)| c.caller == host && c.client_id == id)
            .map(|(r, _)| *r);
        let Some(route) = route else {
            return;
        };
        if frame["type"] == "cancel" {
            self.fail(route, "cancelled");
            return;
        }
        let call = &self.calls[&route];
        let credits = frame["credits"].as_u64().unwrap_or(0);
        if call.definition["mode"] != "stream"
            || credits == 0
            || credits > 16
            || call.credits + credits > 16
        {
            self.fail(route, "protocol_error");
            return;
        }
        let callee = call.callee;
        if let Some(call) = self.calls.get_mut(&route) {
            call.credits += credits;
        }
        if let Some(callee) = callee {
            self.send(
                callee,
                json!({"type":"credit","id":route.to_string(),"credits":credits}),
            );
        }
    }

    fn status(&mut self, host: u64, frame: &Value) {
        let services: Vec<_> = self.hosts.iter().filter_map(|(id, h)| h.identity.as_ref().map(|i| json!({
            "namespace":i.namespace,"service":i.service,"instance":i.instance,"generation":id.to_string(),"transport":h.transport,
            "contract_version":h.contract.as_ref().map(|c| &c["version"]),
            "selected":self.selections.get(&(i.namespace.clone(), i.service.clone())).is_some_and(|s| s.generation == *id),
            "capabilities":h.contract.as_ref().and_then(|c| c["methods"].as_object()).map(|methods|
                methods.iter().map(|(name, definition)| json!({"method":name,"mode":definition["mode"]})).collect::<Vec<_>>()).unwrap_or_default(),
            "state":if h.drained {"drained"} else if h.drain.is_some() {"draining"} else if h.ready {"ready"} else if h.contract.is_some() {"registered"} else {"connected"},
            "active_calls":self.busy(*id),"max_concurrency":h.concurrency
        }))).collect();
        let calls: Vec<_> = self.calls.iter().map(|(id, c)| json!({
            "id":id.to_string(),"caller_generation":c.caller.to_string(),"callee_generation":c.callee.map(|id| id.to_string()),
            "namespace":c.namespace,"service":c.service,"method":c.method,"contract_version":c.version,
            "trace_id":c.trace,"parent_id":c.parent,
            "state":if c.callee.is_some() {"active"} else {"waiting"},"credits":c.credits,
            "remaining_ms":c.deadline.saturating_duration_since(Instant::now()).as_millis()
        })).collect();
        self.send(host, json!({"type":"status","id":frame["id"],"services":services,"calls":calls,"recent":self.recent,
            "limits":{"connections":CONNECTIONS,"active_calls":ACTIVE,"waiting_calls":WAITING,"outbound_frames":OUTBOUND,
            "frame_bytes":MAX_FRAME_BYTES,"deadline_ms":60000,"heartbeat_expiry_ms":5000,
            "diagnostic_drops":diagnostics::dropped()}}));
    }
}

async fn connection(
    connection: transport::Connection,
    id: u64,
    events: mpsc::Sender<Event>,
    mut outbound: mpsc::Receiver<Vec<u8>>,
    mut closed: watch::Receiver<()>,
) {
    let transport::Connection {
        mut reader,
        mut writer,
    } = connection;
    let mut writing_frame = false;
    let result = loop {
        // Arbitrate one message at a time, retaining the original TCP policy.
        // A continuously buffered reader must not monopolize the connection.
        tokio::select! {
            biased;
            _ = closed.changed() => break Ok(()),
            bytes = outbound.recv() => {
                let Some(bytes) = bytes else { break Ok(()); };
                writing_frame = true;
                let sent = tokio::select! {
                    biased;
                    _ = closed.changed() => break Ok(()),
                    result = writer.send_serialized(bytes) => result,
                };
                if let Err(error) = sent { break Err(error); }
                writing_frame = false;
            }
            received = reader.recv() => {
                let bytes = match received {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(error),
                };
                let frame = match serde_json::from_slice::<Value>(&bytes) {
                    Ok(frame) => frame,
                    Err(error) => break Err(io::Error::new(io::ErrorKind::InvalidData,error)),
                };
                let admitted = tokio::select! {
                    biased;
                    _ = closed.changed() => break Ok(()),
                    result = timeout(LIVENESS, events.send(Event::Frame(id,frame))) => result,
                };
                match admitted {
                    Ok(Ok(())) => (),
                    Ok(Err(_)) => break Err(io::Error::new(io::ErrorKind::BrokenPipe,"routing actor closed")),
                    Err(_) => break Err(io::Error::new(io::ErrorKind::TimedOut,"session event queue stalled")),
                }
            }
        }
    };
    if let Err(error) = result {
        diagnostics::emit(
            json!({"event":"transport_failure","generation":id.to_string(),"error":error.to_string()}),
        );
    }
    // Retire routing before a potentially slow diagnostic flush.
    if !matches!(
        timeout(LIVENESS, events.send(Event::Gone(id))).await,
        Ok(Ok(()))
    ) {
        diagnostics::emit(
            json!({"event":"session_cleanup_actor_closed","generation":id.to_string()}),
        );
    }
    // One total flush bound, not one timeout per queued frame.
    if writing_frame {
        // WebSocket graceful close may flush buffered application bytes. Abort
        // instead after an interrupted write; its external outcome is uncertain.
        return;
    }
    let flush = timeout(Duration::from_millis(100), async {
        while let Ok(bytes) = outbound.try_recv() {
            let diagnostic =
                serde_json::from_slice::<Value>(&bytes).is_ok_and(|frame| frame["type"] == "error");
            if diagnostic {
                writer.send_serialized(bytes).await?;
            }
        }
        writer.close().await
    })
    .await;
    if !matches!(flush, Ok(Ok(()))) {
        diagnostics::emit(json!({"event":"transport_close_failed","generation":id.to_string()}));
    }
}

/// Serve a bound loopback listener until shutdown completes.
///
/// All network tasks are joined or aborted on shutdown. Errors reject non-loopback
/// listeners or propagate listener failures; no detached connection tasks survive.
pub async fn serve(listener: TcpListener, shutdown: impl Future<Output = ()>) -> io::Result<()> {
    serve_with_transports(listener, None, Vec::new(), shutdown).await
}

struct Managed {
    process: Option<process::ManagedProcess>,
    config: process::ProcessConfig,
    started: Instant,
    generation: Option<u64>,
    ready: bool,
}

fn attach(
    actor: &mut Actor,
    tasks: &mut JoinSet<()>,
    events: &mpsc::Sender<Event>,
    generation: &mut u64,
    transport: transport::Connection,
    kind: &'static str,
    expected: Option<Identity>,
) -> io::Result<u64> {
    *generation = generation
        .checked_add(1)
        .ok_or_else(|| io::Error::other("generation exhausted"))?;
    let id = *generation;
    let (out, outbound) = mpsc::channel(OUTBOUND);
    let (close, closed) = watch::channel(());
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
    let events = events.clone();
    tasks.spawn(async move {
        connection(transport, id, events, outbound, closed).await;
    });
    Ok(id)
}

/// Serve all local transports through one session/routing actor.
/// Managed commands are persistent services, not per-request processes.
pub async fn serve_with_transports(
    listener: TcpListener,
    websocket: Option<TcpListener>,
    configs: Vec<process::ProcessConfig>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    loopback_address(&listener.local_addr()?.to_string())?;
    if let Some(listener) = &websocket {
        loopback_address(&listener.local_addr()?.to_string())?;
    }
    if configs.len() > CONNECTIONS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many managed services",
        ));
    }
    let mut identities = HashSet::new();
    for config in &configs {
        config.validate()?;
        if !identities.insert((&config.namespace, &config.service, &config.instance)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate managed process identity",
            ));
        }
    }
    let (events, mut incoming) = mpsc::channel(EVENTS);
    let mut tasks = JoinSet::new();
    let mut handshakes = JoinSet::<io::Result<transport::Connection>>::new();
    let mut cleanup = JoinSet::new();
    let mut actor = Actor::default();
    for config in &configs {
        actor.managed_identities.insert(
            (
                config.namespace.clone(),
                config.service.clone(),
                config.instance.clone(),
            ),
            Reservation {
                generation: None,
                transport: match config.transport {
                    process::ProcessTransport::Stdio => "stdio",
                    process::ProcessTransport::Tcp => "tcp",
                },
                ready: false,
            },
        );
    }
    let mut generation = 0_u64;
    let mut managed = Vec::<Managed>::new();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tokio::pin!(shutdown);
    let startup = (|| -> io::Result<()> {
        for mut config in configs {
            // The actual bound TCP port also works with --listen ...:0.
            config.env.insert(
                "LENSO_RUNTIME_PORT".into(),
                listener.local_addr()?.port().to_string(),
            );
            config
                .env
                .insert("LENSO_RUNTIME_NAMESPACE".into(), config.namespace.clone());
            config
                .env
                .insert("LENSO_RUNTIME_SERVICE".into(), config.service.clone());
            config
                .env
                .insert("LENSO_RUNTIME_INSTANCE".into(), config.instance.clone());
            let mut child = process::spawn(&config)?;
            diagnostics::emit(
                json!({"event":"process_started","pid":child.pid,"service":config.service,"instance":config.instance}),
            );
            let id = if let Some(connection) = child.connection.take() {
                let expected = Identity {
                    namespace: config.namespace.clone(),
                    service: config.service.clone(),
                    instance: config.instance.clone(),
                };
                let id = attach(
                    &mut actor,
                    &mut tasks,
                    &events,
                    &mut generation,
                    connection,
                    "stdio",
                    Some(expected),
                )?;
                diagnostics::emit(
                    json!({"event":"process_connected","pid":child.pid,"generation":id.to_string()}),
                );
                Some(id)
            } else {
                None
            };
            managed.push(Managed {
                process: Some(child),
                config,
                started: Instant::now(),
                generation: id,
                ready: false,
            });
        }
        Ok(())
    })();
    let mut result = startup;
    while result.is_ok() {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = tick.tick() => {
                actor.maintenance();
                for entry in &mut managed {
                    let Some(child) = entry.process.as_mut() else { continue; };
                    if entry.generation.is_none() {
                        entry.generation = actor.managed_identities.get(&(entry.config.namespace.clone(),entry.config.service.clone(),entry.config.instance.clone())).and_then(|reservation|reservation.generation);
                        if let Some(id) = entry.generation {
                            diagnostics::emit(json!({"event":"process_connected","pid":child.pid,"generation":id.to_string()}));
                        }
                    }
                    if entry.generation.and_then(|id|actor.hosts.get(&id)).and_then(|h|h.identity.as_ref())
                        .is_some_and(|identity| identity.namespace != entry.config.namespace ||
                            identity.service != entry.config.service || identity.instance != entry.config.instance) {
                        result = Err(io::Error::new(io::ErrorKind::InvalidData,"managed service identity mismatch"));
                        break;
                    }
                    if !entry.ready && actor.managed_identities.get(&(entry.config.namespace.clone(),entry.config.service.clone(),entry.config.instance.clone())).is_some_and(|reservation|reservation.ready) {
                        entry.ready = true;
                        diagnostics::emit(json!({"event":"process_ready","pid":child.pid,"generation":entry.generation.map(|id|id.to_string())}));
                    }
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            diagnostics::emit(json!({"event":"process_exited","pid":child.pid,"status":status.to_string(),"was_ready":entry.ready}));
                            if let Some(id) = entry.generation { actor.disconnect(id); }
                            if let Some(mut child) = entry.process.take() {
                                cleanup.spawn(async move { child.stop().await });
                            }
                            if !entry.ready { result = Err(io::Error::other("managed service exited before ready")); break; }
                        }
                        Err(error) => { result = Err(error); break; }
                        Ok(None) if !entry.ready && entry.started.elapsed() >= Duration::from_millis(entry.config.ready_timeout_ms) => {
                            result = Err(io::Error::new(io::ErrorKind::TimedOut, "managed service ready timeout"));
                            break;
                        }
                        Ok(None) => (),
                    }
                }
            },
            Some(event) = incoming.recv() => {
                match event {
                    Event::Frame(id, frame) => actor.frame(id, frame),
                    Event::Gone(id) => actor.disconnect(id),
                }
                actor.maintenance();
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = joined { result = Err(io::Error::other(error)); }
            }
            Some(joined) = cleanup.join_next(), if !cleanup.is_empty() => {
                match joined {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => result = Err(error),
                    Err(error) => result = Err(io::Error::other(error)),
                }
            }
            Some(joined) = handshakes.join_next(), if !handshakes.is_empty() => {
                match joined {
                    Ok(Ok(connection)) => {
                        if let Err(error) = attach(&mut actor, &mut tasks, &events, &mut generation, connection, "websocket", None) {
                            result = Err(error);
                        }
                    }
                    Ok(Err(error)) => diagnostics::emit(json!({"event":"websocket_handshake_failed","error":error.to_string()})),
                    Err(error) => result = Err(io::Error::other(error)),
                }
            }
            accepted = listener.accept(), if tasks.len() + handshakes.len() < CONNECTIONS => {
                match accepted.and_then(|(stream,_)| transport::Connection::tcp(stream)) {
                    Ok(connection) => if let Err(error) = attach(&mut actor, &mut tasks, &events, &mut generation, connection, "tcp", None) { result = Err(error); },
                    Err(error) => result = Err(error),
                }
            }
            accepted = async { match &websocket { Some(listener) => listener.accept().await, None => std::future::pending().await } },
                if tasks.len() + handshakes.len() < CONNECTIONS => {
                match accepted {
                    Ok((stream,_)) => { handshakes.spawn(async move {
                        timeout(LIVENESS, transport::accept_websocket(stream)).await
                            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut,"websocket handshake timeout"))?
                    }); }
                    Err(error) => result = Err(error),
                }
            }
        }
    }
    handshakes.abort_all();
    while handshakes.join_next().await.is_some() {}
    actor.shutting_down = true;
    // Keep the actor alive while assigned managed work drains. No lifecycle frame
    // is invented: the child's normal signal handler still calls SDK drain.
    let drain_bound = managed
        .iter()
        .filter(|m| m.process.is_some())
        .map(|m| m.config.shutdown_timeout_ms)
        .max()
        .unwrap_or(0);
    let deadline = Instant::now() + Duration::from_millis(drain_bound);
    for host in actor.hosts.values_mut() {
        host.ready = false;
        host.drain = Some(host.drain.map_or(deadline, |old| old.min(deadline)));
    }
    while !actor.calls.is_empty() && Instant::now() < deadline {
        tokio::select! {
            _ = tick.tick() => actor.maintenance(),
            Some(event) = incoming.recv() => match event { Event::Frame(id,frame)=>actor.frame(id,frame), Event::Gone(id)=>actor.disconnect(id) },
        }
    }
    actor.maintenance();
    let remaining: Vec<_> = actor.calls.keys().copied().collect();
    for route in remaining {
        actor.fail(route, "deadline_exceeded");
    }
    for mut entry in managed {
        if let Some(mut child) = entry.process.take() {
            cleanup.spawn(async move { child.stop().await });
        }
    }
    while !cleanup.is_empty() {
        tokio::select! {
            Some(joined) = cleanup.join_next() => {
                let failure = match joined { Ok(Ok(()))=>None, Ok(Err(error))=>Some(error), Err(error)=>Some(io::Error::other(error)) };
                if let Some(error) = failure {
                    diagnostics::emit(json!({"event":"process_cleanup_failed","error":error.to_string()}));
                    if result.is_ok() { result = Err(error); }
                }
            }
            Some(event) = incoming.recv() => match event { Event::Frame(id,frame)=>actor.frame(id,frame), Event::Gone(id)=>actor.disconnect(id) },
            _ = tick.tick() => actor.maintenance(),
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

/// Parse the CLI listener address, accepting only numeric loopback IP addresses.
pub fn loopback_address(value: &str) -> io::Result<SocketAddr> {
    let address: SocketAddr = value
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid listen address"))?;
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "loopback listener required",
        ));
    }
    Ok(address)
}
