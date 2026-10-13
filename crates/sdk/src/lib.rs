//! Bounded, single-connection runtime host and typed caller. No replay or retries.
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{mpsc, oneshot, watch, Semaphore},
    time::{timeout, timeout_at, Instant},
};

struct BoundedJson {
    bytes: Vec<u8>,
}
impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = (protocol::MAX_FRAME_BYTES - 1) - self.bytes.len();
        if bytes.len() > remaining {
            return Err(std::io::Error::other("JSON exceeds frame budget"));
        }
        let required = self.bytes.len() + bytes.len();
        if required > self.bytes.capacity() {
            let capacity = required
                .max(self.bytes.capacity().saturating_mul(2))
                .clamp(1024, protocol::MAX_FRAME_BYTES - 1);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded_json<T: Serialize>(value: &T, code: &str) -> Result<Vec<u8>, RuntimeError> {
    let mut writer = BoundedJson { bytes: Vec::new() };
    serde_json::to_writer(&mut writer, value).map_err(|_| RuntimeError::new(code))?;
    Ok(writer.bytes)
}
fn bounded_value<T: Serialize>(value: &T, code: &str) -> Result<Value, RuntimeError> {
    let bytes = bounded_json(value, code)?;
    serde_json::from_slice(&bytes).map_err(|_| RuntimeError::new(code))
}
fn bounded_frame(frame: &Value, code: &str) -> Result<Vec<u8>, RuntimeError> {
    let mut bytes = bounded_json(frame, code)?;
    bytes.reserve_exact(1);
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct RuntimeError {
    pub code: String,
    pub message: String,
    pub details: Option<Value>,
}
impl RuntimeError {
    pub fn new(code: &str) -> Self {
        Self {
            code: code.into(),
            message: code.into(),
            details: None,
        }
    }
    pub fn business<E: Serialize>(details: E) -> Self {
        match bounded_value(&details, "invalid_output") {
            Ok(details) => Self {
                code: "business".into(),
                message: "Business operation failed".into(),
                details: Some(details),
            },
            Err(_) => Self::new("invalid_output"),
        }
    }
    pub fn business_details<E: DeserializeOwned>(&self) -> Option<E> {
        if self.code != "business" {
            return None;
        }
        serde_json::from_value(self.details.clone()?).ok()
    }
}
#[derive(Debug)]
pub struct BusinessError<E>(pub E);
#[derive(Clone)]
pub struct Cancellation {
    sender: watch::Sender<bool>,
}
impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}
impl Cancellation {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        let _ = receiver.wait_for(|v| *v).await;
    }
}
#[derive(Clone)]
pub struct HandlerContext {
    pub cancellation: Cancellation,
    pub call_id: String,
    pub trace_id: String,
    pub parent_id: Option<String>,
    pub caller: Value,
}
pub struct HostOptions {
    pub namespace: String,
    pub service: String,
    pub instance: String,
    pub port: u16,
    pub contract: Value,
    pub max_concurrency: usize,
}
impl HostOptions {
    /// Validate local configuration without opening a connection.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.port == 0 || !(1..=64).contains(&self.max_concurrency) {
            return Err(RuntimeError::new("invalid_input"));
        }
        protocol::validate_host_frame(&json!({
            "type": "hello", "version": 1,
            "namespace": self.namespace, "service": self.service, "instance": self.instance,
        }))
        .map_err(|_| RuntimeError::new("invalid_input"))?;
        protocol::validate_contract(&self.contract)
            .map_err(|_| RuntimeError::new("contract_mismatch"))
    }
}
#[derive(Clone)]
pub struct Target {
    pub namespace: String,
    pub service: String,
    pub contract_version: String,
}
#[derive(Clone)]
pub struct CallOptions {
    pub timeout_ms: u64,
    pub cancellation: Option<Cancellation>,
    pub trace_id: Option<String>,
    pub parent_id: Option<String>,
}
impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,
            cancellation: None,
            trace_id: None,
            parent_id: None,
        }
    }
}
type HandlerFuture = Pin<Box<dyn Future<Output = Result<Option<Value>, RuntimeError>> + Send>>;
type Handler =
    Arc<dyn Fn(Value, HandlerContext, StreamSender<Value>) -> HandlerFuture + Send + Sync>;
#[derive(Default)]
pub struct Handlers {
    methods: HashMap<String, Handler>,
    modes: HashMap<String, &'static str>,
    duplicate: bool,
}
impl Handlers {
    pub fn new() -> Self {
        Self::default()
    }
    /// Check exact names and modes, including duplicate registration attempts.
    pub fn validate(&self, contract: &Value) -> Result<(), RuntimeError> {
        protocol::validate_contract(contract)
            .map_err(|_| RuntimeError::new("contract_mismatch"))?;
        let methods = contract["methods"]
            .as_object()
            .ok_or_else(|| RuntimeError::new("contract_mismatch"))?;
        if self.duplicate
            || methods.len() != self.methods.len()
            || methods.iter().any(|(name, definition)| {
                self.modes.get(name).copied() != definition["mode"].as_str()
            })
        {
            return Err(RuntimeError::new("contract_mismatch"));
        }
        Ok(())
    }
    /// Register an ordinary async business function with typed input, output and errors.
    pub fn unary<I, O, E, F, Fut>(&mut self, name: &str, handler: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        E: Serialize + Send + 'static,
        F: Fn(I, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, BusinessError<E>>> + Send + 'static,
    {
        self.duplicate |= self.modes.insert(name.into(), "unary").is_some();
        let handler = Arc::new(handler);
        self.methods.insert(
            name.into(),
            Arc::new(move |input, context, _| {
                let handler = handler.clone();
                Box::pin(async move {
                    let input = serde_json::from_value(input)
                        .map_err(|_| RuntimeError::new("invalid_input"))?;
                    let output = handler(input, context)
                        .await
                        .map_err(|e| RuntimeError::business(e.0))?;
                    bounded_value(&output, "invalid_output").map(Some)
                })
            }),
        );
    }
    /// `send().await` spends demand credit before a chunk can leave the handler.
    pub fn stream<I, O, F, Fut>(&mut self, name: &str, handler: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I, HandlerContext, StreamSender<O>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), RuntimeError>> + Send + 'static,
    {
        self.duplicate |= self.modes.insert(name.into(), "stream").is_some();
        let handler = Arc::new(handler);
        self.methods.insert(
            name.into(),
            Arc::new(move |input, context, sender| {
                let handler = handler.clone();
                Box::pin(async move {
                    let input = serde_json::from_value(input)
                        .map_err(|_| RuntimeError::new("invalid_input"))?;
                    handler(input, context, sender.typed()).await?;
                    Ok(None)
                })
            }),
        );
    }
}
pub struct StreamSender<T> {
    inner: Arc<Inner>,
    id: String,
    schema: Value,
    credits: Arc<Semaphore>,
    marker: PhantomData<T>,
}
impl<T> StreamSender<T> {
    fn typed<O>(self) -> StreamSender<O> {
        StreamSender {
            inner: self.inner,
            id: self.id,
            schema: self.schema,
            credits: self.credits,
            marker: PhantomData,
        }
    }
}
impl<T: Serialize> StreamSender<T> {
    pub async fn send(&self, output: T) -> Result<(), RuntimeError> {
        let permit = self
            .credits
            .acquire()
            .await
            .map_err(|_| RuntimeError::new("cancelled"))?;
        let output = bounded_value(&output, "invalid_output")?;
        protocol::validate_value(&self.schema, &output)
            .map_err(|_| RuntimeError::new("invalid_output"))?;
        self.inner
            .send(json!({"type":"chunk","id":self.id,"output":output}))?;
        permit.forget();
        Ok(())
    }
}
enum PendingKind {
    Unary(oneshot::Sender<Result<Value, RuntimeError>>),
    Stream {
        chunks: mpsc::Sender<Value>,
        terminal: watch::Sender<Option<Result<(), RuntimeError>>>,
        credit: Arc<AtomicUsize>,
    },
}
struct Pending {
    kind: PendingKind,
    completed: Cancellation,
}
struct Invocation {
    cancellation: Cancellation,
    credits: Arc<Semaphore>,
    stream: bool,
}
struct State {
    pending: HashMap<String, Pending>,
    invocations: HashMap<String, Invocation>,
    executions: usize,
    drained: Option<Drain>,
    handlers: Option<Arc<Handlers>>,
    activation: Option<watch::Sender<Option<Result<(), RuntimeError>>>>,
}
struct Drain {
    deadline: Instant,
    acknowledged: bool,
    completion: watch::Sender<Option<Result<(), RuntimeError>>>,
}
impl State {
    fn finish_drain(&self) {
        if let Some(drain) = &self.drained {
            if drain.acknowledged && self.executions == 0 && drain.completion.borrow().is_none() {
                let result = if Instant::now() >= drain.deadline {
                    Err(RuntimeError::new("deadline_exceeded"))
                } else {
                    Ok(())
                };
                drain.completion.send_replace(Some(result));
            }
        }
    }
}
struct Execution {
    inner: Arc<Inner>,
    id: String,
    cancellation: Cancellation,
    credits: Arc<Semaphore>,
}
impl Drop for Execution {
    fn drop(&mut self) {
        self.credits.close();
        self.cancellation.cancel();
        let mut state = self.inner.state();
        state.invocations.remove(&self.id);
        state.executions -= 1;
        state.finish_drain();
    }
}
struct Inner {
    writer: mpsc::Sender<Vec<u8>>,
    contract: Value,
    host_handles: AtomicUsize,
    closed: Cancellation,
    accepting: AtomicBool,
    sequence: AtomicU64,
    admission: Mutex<()>,
    state: Mutex<State>,
}
impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
    fn send(&self, frame: Value) -> Result<(), RuntimeError> {
        if self.closed.is_cancelled() {
            return Err(RuntimeError::new("connection_lost"));
        }
        let code = match frame["type"].as_str() {
            Some("result" | "chunk" | "end" | "error") => "invalid_output",
            _ => "invalid_input",
        };
        let bytes = bounded_frame(&frame, code)?;
        self.writer.try_send(bytes).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => RuntimeError::new("overloaded"),
            mpsc::error::TrySendError::Closed(_) => {
                let error = RuntimeError::new("connection_lost");
                self.fail(error.clone());
                error
            }
        })
    }
    fn fail(&self, error: RuntimeError) {
        if self.closed.is_cancelled() {
            return;
        }
        self.closed.cancel();
        let mut state = self.state();
        for (_, pending) in state.pending.drain() {
            finish_pending(pending, Err(error.clone()));
        }
        for (_, invocation) in state.invocations.drain() {
            invocation.cancellation.cancel();
            invocation.credits.close();
        }
        if let Some(activation) = &state.activation {
            if activation.borrow().is_none() {
                activation.send_replace(Some(Err(error.clone())));
            }
        }
        if let Some(drain) = &state.drained {
            if drain.completion.borrow().is_none() {
                drain.completion.send_replace(Some(Err(error)));
            }
        }
    }
    fn terminate(&self, id: &str, error: RuntimeError) {
        let pending = self.state().pending.remove(id);
        if let Some(pending) = pending {
            finish_pending(pending, Err(error));
            // Losing a control frame would orphan the remote route until its deadline.
            if let Err(error) = self.send(json!({"type":"cancel","id":id})) {
                self.fail(error);
            }
        }
    }
}
fn finish_pending(pending: Pending, result: Result<Value, RuntimeError>) {
    pending.completed.cancel();
    match pending.kind {
        PendingKind::Unary(sender) => {
            let _ = sender.send(result);
        }
        PendingKind::Stream { terminal, .. } => {
            terminal.send_replace(Some(result.map(|_| ())));
        }
    }
}
pub struct RuntimeHost {
    inner: Arc<Inner>,
}
struct ActivationGuard<'a> {
    inner: &'a Inner,
    completed: bool,
}
impl Drop for ActivationGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.inner.fail(RuntimeError::new("cancelled"));
        }
    }
}
impl Clone for RuntimeHost {
    fn clone(&self) -> Self {
        self.inner.host_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl Drop for RuntimeHost {
    fn drop(&mut self) {
        if self.inner.host_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.fail(RuntimeError::new("connection_lost"));
        }
    }
}
struct CallGuard {
    inner: Arc<Inner>,
    id: String,
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        self.inner
            .terminate(&self.id, RuntimeError::new("cancelled"));
    }
}
impl RuntimeHost {
    pub async fn connect(options: HostOptions, handlers: Handlers) -> Result<Self, RuntimeError> {
        handlers.validate(&options.contract)?;
        let host = Self::connect_registered(options).await?;
        host.activate(handlers).await?;
        Ok(host)
    }

    /// Register a connection with outbound calls available, without publishing readiness.
    pub async fn connect_registered(options: HostOptions) -> Result<Self, RuntimeError> {
        options.validate()?;
        let setup = async {
            let mut socket = TcpStream::connect(("127.0.0.1", options.port))
                .await
                .map_err(|_| RuntimeError::new("connection_lost"))?;
            write_frame(&mut socket, &json!({"type":"hello","version":1,"namespace":options.namespace,"service":options.service,"instance":options.instance})).await?;
            let welcome = read_frame(&mut socket).await?;
            validate_kernel_frame(&welcome)?;
            if welcome["type"] != "welcome" || welcome["version"] != 1 {
                return Err(frame_error(&welcome));
            }
            let heartbeat_ms = welcome["heartbeat_ms"]
                .as_u64()
                .filter(|n| (1..=5000).contains(n))
                .ok_or_else(|| RuntimeError::new("protocol_error"))?;
            write_frame(&mut socket, &json!({"type":"register","contract":options.contract,"max_concurrency":options.max_concurrency})).await?;
            let registered = read_frame(&mut socket).await?;
            validate_kernel_frame(&registered)?;
            if registered["type"] != "registered" {
                return Err(frame_error(&registered));
            }
            Ok((socket, heartbeat_ms))
        };
        let (socket, heartbeat_ms) = timeout(Duration::from_secs(5), setup)
            .await
            .map_err(|_| RuntimeError::new("deadline_exceeded"))??;
        let (writer, mut receiver) = mpsc::channel::<Vec<u8>>(64);
        let inner = Arc::new(Inner {
            writer,
            contract: options.contract.clone(),
            host_handles: AtomicUsize::new(1),
            closed: Cancellation::new(),
            accepting: AtomicBool::new(true),
            sequence: AtomicU64::new(0),
            admission: Mutex::new(()),
            state: Mutex::new(State {
                pending: HashMap::new(),
                invocations: HashMap::new(),
                executions: 0,
                drained: None,
                handlers: None,
                activation: None,
            }),
        });
        let (read, mut write) = socket.into_split();
        let mut read = BufReader::new(read);
        let writer_inner = inner.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = writer_inner.closed.cancelled() => break,
                    bytes = receiver.recv() => {
                        let Some(bytes) = bytes else { break; };
                        tokio::select! {
                            _ = writer_inner.closed.cancelled() => break,
                            result = write.write_all(&bytes) => if result.is_err() { writer_inner.fail(RuntimeError::new("connection_lost")); break; }
                        }
                    }
                }
            }
            let _ = write.shutdown().await;
        });
        let reader_inner = inner.clone();
        tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    _ = reader_inner.closed.cancelled() => break,
                    result = read_frame(&mut read) => result,
                };
                match result.and_then(|frame| receive(&reader_inner, frame, &options)) {
                    Ok(()) => (),
                    Err(error) if error.code == "overloaded" => (),
                    Err(error) => {
                        reader_inner.fail(error);
                        break;
                    }
                }
            }
        });
        let heartbeat_inner = inner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_ms));
            loop {
                tokio::select! {
                    _ = heartbeat_inner.closed.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = heartbeat_inner.send(json!({"type":"heartbeat"})) {
                            if error.code != "overloaded" { break; }
                        }
                    }
                }
            }
        });
        Ok(Self { inner })
    }

    /// Check explicit exports without publishing readiness or changing the connection.
    pub fn validate_handlers(&self, handlers: &Handlers) -> Result<(), RuntimeError> {
        handlers.validate(&self.inner.contract)
    }

    /// Bind the exact registered methods once and await the kernel's ready acknowledgement.
    /// Failed or cancelled activation closes the connection.
    pub async fn activate(&self, handlers: Handlers) -> Result<(), RuntimeError> {
        let mut receiver = {
            let _admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if self.inner.closed.is_cancelled() {
                return Err(RuntimeError::new("connection_lost"));
            }
            if !self.inner.accepting.load(Ordering::Relaxed) {
                return Err(RuntimeError::new("not_ready"));
            }
            if self.inner.state().activation.is_some() {
                return Err(RuntimeError::new("invalid_input"));
            }
            if let Err(error) = handlers.validate(&self.inner.contract) {
                self.inner.fail(error.clone());
                return Err(error);
            }
            let (completion, receiver) = watch::channel(None);
            {
                let mut state = self.inner.state();
                state.handlers = Some(Arc::new(handlers));
                state.activation = Some(completion);
            }
            if let Err(error) = self.inner.send(json!({"type":"ready"})) {
                self.inner.fail(error.clone());
                return Err(error);
            }
            receiver
        };
        let mut guard = ActivationGuard {
            inner: &self.inner,
            completed: false,
        };
        let result = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = receiver.borrow_and_update().clone() {
                    return result;
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::new("connection_lost"))?;
            }
        })
        .await
        .unwrap_or_else(|_| Err(RuntimeError::new("deadline_exceeded")));
        if let Err(error) = &result {
            self.inner.fail(error.clone());
        }
        guard.completed = true;
        result
    }
    fn start(
        &self,
        target: &Target,
        method: &str,
        input: Value,
        options: &CallOptions,
        pending: PendingKind,
    ) -> Result<CallGuard, RuntimeError> {
        // Allocate and enqueue under one lock so concurrent callers cannot reorder IDs.
        let _admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if self.inner.closed.is_cancelled() {
            return Err(RuntimeError::new("connection_lost"));
        }
        if !self.inner.accepting.load(Ordering::Relaxed) {
            return Err(RuntimeError::new("not_ready"));
        }
        if !(1..=60000).contains(&options.timeout_ms) {
            return Err(RuntimeError::new("invalid_input"));
        }
        if options
            .cancellation
            .as_ref()
            .is_some_and(Cancellation::is_cancelled)
        {
            return Err(RuntimeError::new("cancelled"));
        }
        let id = self
            .inner
            .sequence
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| RuntimeError::new("overloaded"))?
            .to_string();
        let completed = Cancellation::new();
        {
            let mut state = self.inner.state();
            if state.pending.len() >= 192 {
                return Err(RuntimeError::new("overloaded"));
            }
            state.pending.insert(
                id.clone(),
                Pending {
                    kind: pending,
                    completed: completed.clone(),
                },
            );
        }
        let mut frame = json!({"type":"call","id":id,"namespace":target.namespace,"service":target.service,"method":method,"contract_version":target.contract_version,"input":input,"timeout_ms":options.timeout_ms});
        if let Some(trace) = &options.trace_id {
            frame["trace_id"] = trace.clone().into();
        }
        if let Some(parent) = &options.parent_id {
            frame["parent_id"] = parent.clone().into();
        }
        if let Err(error) = self.inner.send(frame) {
            self.inner.state().pending.remove(&id);
            return Err(error);
        }
        let inner = self.inner.clone();
        let deadline_id = id.clone();
        let cancellation = options.cancellation.clone().unwrap_or_default();
        let deadline = Duration::from_millis(options.timeout_ms);
        tokio::spawn(async move {
            let error = tokio::select! {
                _ = inner.closed.cancelled() => return,
                _ = completed.cancelled() => return,
                _ = cancellation.cancelled() => RuntimeError::new("cancelled"),
                _ = tokio::time::sleep(deadline) => RuntimeError::new("deadline_exceeded"),
            };
            inner.terminate(&deadline_id, error);
        });
        Ok(CallGuard {
            inner: self.inner.clone(),
            id,
        })
    }
    /// Validate against the caller-held method schema, not only serde's Rust types.
    pub async fn call_contract<I: Serialize, O: DeserializeOwned>(
        &self,
        target: &Target,
        method: &str,
        input: I,
        options: CallOptions,
        definition: &Value,
    ) -> Result<O, RuntimeError> {
        let input = bounded_value(&input, "invalid_input")?;
        protocol::validate_value(&definition["input"], &input)
            .map_err(|_| RuntimeError::new("invalid_input"))?;
        let output: Value = self
            .call(target, method, input, options)
            .await
            .map_err(|error| validate_business(error, definition))?;
        protocol::validate_value(&definition["output"], &output)
            .map_err(|_| RuntimeError::new("invalid_output"))?;
        serde_json::from_value(output).map_err(|_| RuntimeError::new("invalid_output"))
    }
    /// Invalid chunks terminalize the stream and cancel its route immediately.
    pub fn stream_contract<I: Serialize, O: DeserializeOwned>(
        &self,
        target: &Target,
        method: &str,
        input: I,
        options: CallOptions,
        definition: &Value,
    ) -> Result<ResponseStream<O>, RuntimeError> {
        let input = bounded_value(&input, "invalid_input")?;
        protocol::validate_value(&definition["input"], &input)
            .map_err(|_| RuntimeError::new("invalid_input"))?;
        let mut stream = self.stream(target, method, input, options)?;
        stream.definition = Some(definition.clone());
        Ok(stream)
    }
    /// Dropping the future cancels its route, including while queued.
    pub async fn call<I: Serialize, O: DeserializeOwned>(
        &self,
        target: &Target,
        method: &str,
        input: I,
        options: CallOptions,
    ) -> Result<O, RuntimeError> {
        let input = bounded_value(&input, "invalid_input")?;
        let (sender, receiver) = oneshot::channel();
        let _guard = self.start(target, method, input, &options, PendingKind::Unary(sender))?;
        let output = receiver
            .await
            .map_err(|_| RuntimeError::new("connection_lost"))??;
        serde_json::from_value(output).map_err(|_| RuntimeError::new("invalid_output"))
    }
    pub fn stream<I: Serialize, O: DeserializeOwned>(
        &self,
        target: &Target,
        method: &str,
        input: I,
        options: CallOptions,
    ) -> Result<ResponseStream<O>, RuntimeError> {
        let input = bounded_value(&input, "invalid_input")?;
        let (chunks, receiver) = mpsc::channel(1);
        let (terminal, terminal_receiver) = watch::channel(None);
        let credit = Arc::new(AtomicUsize::new(0));
        let guard = self.start(
            target,
            method,
            input,
            &options,
            PendingKind::Stream {
                chunks,
                terminal,
                credit: credit.clone(),
            },
        )?;
        Ok(ResponseStream {
            guard,
            receiver,
            terminal: terminal_receiver,
            credit,
            done: false,
            waiting: false,
            definition: None,
            marker: PhantomData,
        })
    }
    pub async fn drain(&self, timeout_ms: u64) -> Result<(), RuntimeError> {
        if !(1..=60000).contains(&timeout_ms) {
            return Err(RuntimeError::new("invalid_input"));
        }
        let (mut receiver, deadline, start) = {
            let _admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            self.inner.accepting.store(false, Ordering::Relaxed);
            let mut state = self.inner.state();
            if let Some(activation) = &state.activation {
                if activation.borrow().is_none() {
                    activation.send_replace(Some(Err(RuntimeError::new("not_ready"))));
                }
            }
            let start = state.drained.is_none();
            if start {
                let (completion, _) = watch::channel(None);
                state.drained = Some(Drain {
                    deadline: Instant::now() + Duration::from_millis(timeout_ms + 100),
                    acknowledged: self.inner.closed.is_cancelled(),
                    completion,
                });
                state.finish_drain();
            }
            let drain = state
                .drained
                .as_ref()
                .ok_or_else(|| RuntimeError::new("protocol_error"))?;
            (drain.completion.subscribe(), drain.deadline, start)
        };
        if start && !self.inner.closed.is_cancelled() {
            if let Err(error) = self
                .inner
                .send(json!({"type":"drain","timeout_ms":timeout_ms}))
            {
                let state = self.inner.state();
                let drain = state
                    .drained
                    .as_ref()
                    .ok_or_else(|| RuntimeError::new("protocol_error"))?;
                if drain.completion.borrow().is_none() {
                    drain.completion.send_replace(Some(Err(error)));
                }
            }
        }
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            if timeout_at(deadline, receiver.changed()).await.is_err() {
                let error = RuntimeError::new("deadline_exceeded");
                {
                    let state = self.inner.state();
                    let drain = state
                        .drained
                        .as_ref()
                        .ok_or_else(|| RuntimeError::new("protocol_error"))?;
                    if drain.completion.borrow().is_none() {
                        drain.completion.send_replace(Some(Err(error.clone())));
                    }
                }
                self.inner.fail(error);
            }
        }
    }
    pub fn close(&self) {
        self.inner.fail(RuntimeError::new("connection_lost"));
    }
}
pub struct ResponseStream<T> {
    guard: CallGuard,
    receiver: mpsc::Receiver<Value>,
    terminal: watch::Receiver<Option<Result<(), RuntimeError>>>,
    credit: Arc<AtomicUsize>,
    done: bool,
    waiting: bool,
    definition: Option<Value>,
    marker: PhantomData<T>,
}
fn validate_business(error: RuntimeError, definition: &Value) -> RuntimeError {
    if error.code == "business"
        && error
            .details
            .as_ref()
            .is_none_or(|details| protocol::validate_value(&definition["error"], details).is_err())
    {
        RuntimeError::new("invalid_output")
    } else {
        error
    }
}
impl<T: DeserializeOwned> ResponseStream<T> {
    fn decode(&mut self, value: Value) -> Result<Option<T>, RuntimeError> {
        let result = (|| {
            if let Some(definition) = &self.definition {
                protocol::validate_value(&definition["output"], &value)
                    .map_err(|_| RuntimeError::new("invalid_output"))?;
            }
            serde_json::from_value(value)
                .map(Some)
                .map_err(|_| RuntimeError::new("invalid_output"))
        })();
        if let Err(error) = &result {
            self.done = true;
            self.waiting = false;
            self.credit.store(0, Ordering::Release);
            self.receiver.close();
            self.guard.inner.terminate(&self.guard.id, error.clone());
        }
        result
    }
    fn terminal_result(&self, result: Result<(), RuntimeError>) -> Result<(), RuntimeError> {
        result.map_err(|error| match &self.definition {
            Some(definition) => validate_business(error, definition),
            None => error,
        })
    }
    fn finish(&mut self, result: Result<(), RuntimeError>) -> Result<Option<T>, RuntimeError> {
        self.done = true;
        self.waiting = false;
        self.credit.store(0, Ordering::Release);
        self.receiver.close();
        let buffered = self.receiver.try_recv().ok();
        self.terminal_result(result)?;
        match buffered {
            Some(value) => self.decode(value),
            None => Ok(None),
        }
    }
    /// Exactly one credit per next; dropping this stream cancels the route.
    pub async fn next(&mut self) -> Result<Option<T>, RuntimeError> {
        if self.done {
            return Ok(None);
        }
        let terminal = self.terminal.borrow().clone();
        if let Some(result) = terminal {
            return self.finish(result);
        }
        if !self.waiting {
            self.credit.store(1, Ordering::Release);
            if let Err(error) = self
                .guard
                .inner
                .send(json!({"type":"credit","id":self.guard.id,"credits":1}))
            {
                self.credit.store(0, Ordering::Release);
                self.done = true;
                self.guard.inner.terminate(&self.guard.id, error.clone());
                return Err(error);
            }
            self.waiting = true;
        }
        tokio::select! {
            biased;
            // A known terminal error takes precedence over a buffered chunk.
            _ = self.terminal.changed() => {
                let result = self.terminal.borrow().clone().unwrap_or(Err(RuntimeError::new("connection_lost")));
                self.finish(result)
            }
            value = self.receiver.recv() => {
                self.waiting = false;
                if let Some(value) = value { self.decode(value) }
                else {
                    let result = self.terminal.borrow().clone().unwrap_or(Err(RuntimeError::new("connection_lost")));
                    self.finish(result)
                }
            }
        }
    }
}
async fn write_frame(socket: &mut TcpStream, frame: &Value) -> Result<(), RuntimeError> {
    let bytes = bounded_frame(frame, "invalid_input")?;
    socket
        .write_all(&bytes)
        .await
        .map_err(|_| RuntimeError::new("connection_lost"))
}
async fn read_frame<R: AsyncReadExt + Unpin>(read: &mut R) -> Result<Value, RuntimeError> {
    // Bound allocation before newline, including a peer that never terminates a frame.
    let mut bytes = Vec::new();
    loop {
        let byte = read
            .read_u8()
            .await
            .map_err(|_| RuntimeError::new("connection_lost"))?;
        if bytes.len() + 1 > protocol::MAX_FRAME_BYTES {
            return Err(RuntimeError::new("protocol_error"));
        }
        if byte == b'\n' {
            break;
        }
        bytes.push(byte);
    }
    serde_json::from_slice(&bytes).map_err(|_| RuntimeError::new("protocol_error"))
}
fn frame_error(frame: &Value) -> RuntimeError {
    RuntimeError {
        code: frame["code"].as_str().unwrap_or("protocol_error").into(),
        message: frame["message"]
            .as_str()
            .unwrap_or("Malformed frame")
            .into(),
        details: frame.get("details").cloned(),
    }
}
fn validate_kernel_frame(frame: &Value) -> Result<(), RuntimeError> {
    let object = frame
        .as_object()
        .ok_or_else(|| RuntimeError::new("protocol_error"))?;
    let kind = frame["type"]
        .as_str()
        .ok_or_else(|| RuntimeError::new("protocol_error"))?;
    let (required, optional): (&[&str], &[&str]) = match kind {
        "welcome" => (&["version", "generation", "heartbeat_ms"], &[]),
        "registered" | "ready" | "drained" => (&[], &[]),
        "invoke" => (
            &["id", "method", "input", "timeout_ms", "trace_id", "caller"],
            &["parent_id"],
        ),
        "credit" => (&["id", "credits"], &[]),
        "cancel" | "end" => (&["id"], &[]),
        "result" | "chunk" => (&["id", "output"], &[]),
        "error" => (&["code", "message"], &["id", "details"]),
        _ => return Err(RuntimeError::new("protocol_error")),
    };
    if required.iter().any(|key| !object.contains_key(*key))
        || object.keys().any(|key| {
            key != "type" && !required.contains(&key.as_str()) && !optional.contains(&key.as_str())
        })
    {
        return Err(RuntimeError::new("protocol_error"));
    }
    for key in [
        "id",
        "method",
        "generation",
        "trace_id",
        "parent_id",
        "code",
        "message",
    ] {
        if object.get(key).is_some_and(|value| !value.is_string()) {
            return Err(RuntimeError::new("protocol_error"));
        }
    }
    if kind == "invoke" {
        let caller = frame["caller"]
            .as_object()
            .ok_or_else(|| RuntimeError::new("protocol_error"))?;
        if caller.len() != 3
            || ["namespace", "service", "instance"]
                .iter()
                .any(|key| !caller.get(*key).is_some_and(Value::is_string))
        {
            return Err(RuntimeError::new("protocol_error"));
        }
    }
    Ok(())
}
fn receive(inner: &Arc<Inner>, frame: Value, options: &HostOptions) -> Result<(), RuntimeError> {
    validate_kernel_frame(&frame)?;
    let kind = frame["type"]
        .as_str()
        .ok_or_else(|| RuntimeError::new("protocol_error"))?;
    if kind == "ready" {
        let state = inner.state();
        let activation = state
            .activation
            .as_ref()
            .ok_or_else(|| RuntimeError::new("protocol_error"))?;
        if activation.borrow().is_some() {
            return Err(RuntimeError::new("protocol_error"));
        }
        activation.send_replace(Some(Ok(())));
        return Ok(());
    }
    if kind == "error" && frame.get("id").is_none() {
        return Err(frame_error(&frame));
    }
    if kind == "drained" {
        let mut state = inner.state();
        let drain = state
            .drained
            .as_mut()
            .ok_or_else(|| RuntimeError::new("protocol_error"))?;
        if drain.acknowledged {
            return Err(RuntimeError::new("protocol_error"));
        }
        drain.acknowledged = true;
        state.finish_drain();
        return Ok(());
    }
    let id = frame["id"]
        .as_str()
        .ok_or_else(|| RuntimeError::new("protocol_error"))?
        .to_owned();
    match kind {
        "invoke" => {
            let method = frame["method"]
                .as_str()
                .ok_or_else(|| RuntimeError::new("protocol_error"))?;
            let definition = options.contract["methods"]
                .get(method)
                .cloned()
                .ok_or_else(|| RuntimeError::new("protocol_error"))?;
            let handler = {
                let state = inner.state();
                let handlers = state
                    .handlers
                    .as_ref()
                    .ok_or_else(|| RuntimeError::new("protocol_error"))?;
                if !state
                    .activation
                    .as_ref()
                    .is_some_and(|a| matches!(&*a.borrow(), Some(Ok(()))))
                {
                    return Err(RuntimeError::new("protocol_error"));
                }
                handlers.methods.get(method).cloned()
            };
            let deadline = frame["timeout_ms"]
                .as_u64()
                .filter(|n| (1..=60000).contains(n))
                .ok_or_else(|| RuntimeError::new("protocol_error"))?;
            let context = HandlerContext {
                cancellation: Cancellation::new(),
                call_id: id.to_string(),
                trace_id: frame["trace_id"]
                    .as_str()
                    .ok_or_else(|| RuntimeError::new("protocol_error"))?
                    .into(),
                parent_id: frame["parent_id"].as_str().map(str::to_owned),
                caller: frame["caller"].clone(),
            };
            let credits = Arc::new(Semaphore::new(0));
            let execution = {
                let mut state = inner.state();
                if state.invocations.contains_key(&id) {
                    return Err(RuntimeError::new("protocol_error"));
                }
                if !inner.accepting.load(Ordering::Relaxed)
                    || state.executions >= options.max_concurrency
                {
                    drop(state);
                    inner.send(json!({"type":"error","id":id,"code":"overloaded","message":"Host is busy"}))?;
                    return Ok(());
                }
                state.invocations.insert(
                    id.clone(),
                    Invocation {
                        cancellation: context.cancellation.clone(),
                        credits: credits.clone(),
                        stream: definition["mode"] == "stream",
                    },
                );
                state.executions += 1;
                Execution {
                    inner: inner.clone(),
                    id: id.clone(),
                    cancellation: context.cancellation.clone(),
                    credits: credits.clone(),
                }
            };
            let inner = inner.clone();
            tokio::spawn(async move {
                // Declared first so handler futures and their permits drop before this lease.
                let _execution = execution;
                let operation = async {
                    protocol::validate_value(&definition["input"], &frame["input"])
                        .map_err(|_| RuntimeError::new("invalid_input"))?;
                    let handler = handler.ok_or_else(|| RuntimeError::new("not_ready"))?;
                    let sender = StreamSender {
                        inner: inner.clone(),
                        id: id.clone(),
                        schema: definition["output"].clone(),
                        credits: credits.clone(),
                        marker: PhantomData,
                    };
                    let output = handler(frame["input"].clone(), context.clone(), sender).await?;
                    if definition["mode"] == "unary" {
                        let output = output.ok_or_else(|| RuntimeError::new("invalid_output"))?;
                        protocol::validate_value(&definition["output"], &output)
                            .map_err(|_| RuntimeError::new("invalid_output"))?;
                        inner.send(json!({"type":"result","id":id,"output":output}))
                    } else {
                        if output.is_some() {
                            return Err(RuntimeError::new("invalid_output"));
                        }
                        inner.send(json!({"type":"end","id":id}))
                    }
                };
                let result = tokio::select! {
                    result = timeout_at(Instant::now() + Duration::from_millis(deadline), operation) => result.unwrap_or_else(|_| Err(RuntimeError::new("deadline_exceeded"))),
                    _ = context.cancellation.cancelled() => Err(RuntimeError::new("cancelled")),
                };
                if let Err(mut error) = result {
                    if error.code == "business"
                        && protocol::validate_value(
                            &definition["error"],
                            error.details.as_ref().unwrap_or(&Value::Null),
                        )
                        .is_err()
                    {
                        error = RuntimeError::new("invalid_output");
                    }
                    let mut response =
                        json!({"type":"error","id":id,"code":error.code,"message":error.message});
                    if let Some(details) = error.details {
                        response["details"] = details;
                    }
                    let _ = inner.send(response);
                }
            });
        }
        "credit" => {
            let count = frame["credits"]
                .as_u64()
                .filter(|n| (1..=16).contains(n))
                .ok_or_else(|| RuntimeError::new("protocol_error"))?
                as usize;
            let state = inner.state();
            if let Some(invocation) = state.invocations.get(&id) {
                if !invocation.stream || invocation.credits.available_permits() + count > 16 {
                    return Err(RuntimeError::new("protocol_error"));
                }
                invocation.credits.add_permits(count);
            }
        }
        "cancel" => {
            if let Some(invocation) = inner.state().invocations.get(&id) {
                invocation.cancellation.cancel();
                invocation.credits.close();
            }
        }
        "result" | "error" | "end" => {
            let pending = inner.state().pending.remove(&id);
            if let Some(pending) = pending {
                let result = match kind {
                    "error" => Err(frame_error(&frame)),
                    "end" if matches!(pending.kind, PendingKind::Stream { .. }) => Ok(Value::Null),
                    "result" if matches!(pending.kind, PendingKind::Unary(_)) => frame
                        .get("output")
                        .cloned()
                        .ok_or_else(|| RuntimeError::new("protocol_error")),
                    _ => Err(RuntimeError::new("protocol_error")),
                };
                finish_pending(pending, result);
            }
        }
        "chunk" => {
            let state = inner.state();
            if let Some(pending) = state.pending.get(&id) {
                if let PendingKind::Stream { chunks, credit, .. } = &pending.kind {
                    credit
                        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                        .map_err(|_| RuntimeError::new("protocol_error"))?;
                    chunks
                        .try_send(
                            frame
                                .get("output")
                                .cloned()
                                .ok_or_else(|| RuntimeError::new("protocol_error"))?,
                        )
                        .map_err(|_| RuntimeError::new("protocol_error"))?;
                } else {
                    return Err(RuntimeError::new("protocol_error"));
                }
            }
        }
        _ => return Err(RuntimeError::new("protocol_error")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn fixture() -> (RuntimeHost, TcpStream) {
        fixture_with(Handlers::new(), json!({"version":"1","methods":{}})).await
    }
    async fn fixture_with(handlers: Handlers, contract: Value) -> (RuntimeHost, TcpStream) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connect = tokio::spawn(async move {
            RuntimeHost::connect(
                HostOptions {
                    namespace: "demo".into(),
                    service: "test".into(),
                    instance: "one".into(),
                    port,
                    contract,
                    max_concurrency: 8,
                },
                handlers,
            )
            .await
            .unwrap()
        });
        let (mut peer, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut peer).await.unwrap()["type"], "hello");
        write_frame(
            &mut peer,
            &json!({"type":"welcome","version":1,"generation":"1","heartbeat_ms":1000}),
        )
        .await
        .unwrap();
        assert_eq!(read_frame(&mut peer).await.unwrap()["type"], "register");
        write_frame(&mut peer, &json!({"type":"registered"}))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut peer).await.unwrap()["type"], "ready");
        write_frame(&mut peer, &json!({"type":"ready"}))
            .await
            .unwrap();
        (connect.await.unwrap(), peer)
    }
    async fn next(peer: &mut TcpStream, kind: &str) -> Value {
        loop {
            let frame = timeout(Duration::from_secs(2), read_frame(peer))
                .await
                .unwrap()
                .unwrap();
            if frame["type"] == "heartbeat" {
                continue;
            }
            assert_eq!(frame["type"], kind);
            return frame;
        }
    }
    fn target() -> Target {
        Target {
            namespace: "demo".into(),
            service: "worker".into(),
            contract_version: "1".into(),
        }
    }

    async fn registered_fixture(contract: Value) -> (RuntimeHost, TcpStream) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connect = tokio::spawn(RuntimeHost::connect_registered(HostOptions {
            namespace: "demo".into(),
            service: "test".into(),
            instance: "one".into(),
            port,
            contract,
            max_concurrency: 8,
        }));
        let (mut peer, _) = listener.accept().await.unwrap();
        next(&mut peer, "hello").await;
        write_frame(
            &mut peer,
            &json!({"type":"welcome","version":1,"generation":"1","heartbeat_ms":1000}),
        )
        .await
        .unwrap();
        next(&mut peer, "register").await;
        write_frame(&mut peer, &json!({"type":"registered"}))
            .await
            .unwrap();
        (connect.await.unwrap().unwrap(), peer)
    }

    async fn assert_closed_without_ready(peer: &mut TcpStream) {
        timeout(Duration::from_secs(1), async {
            while let Ok(frame) = read_frame(peer).await {
                assert_eq!(frame["type"], "heartbeat");
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn registered_host_calls_before_ready_and_activates_once_after_ack() {
        let (host, mut peer) = registered_fixture(json!({"version":"1","methods":{}})).await;
        let caller = host.clone();
        let call = tokio::spawn(async move {
            caller
                .call::<_, Value>(&target(), "count", &json!({}), CallOptions::default())
                .await
        });
        let frame = next(&mut peer, "call").await;
        write_frame(
            &mut peer,
            &json!({"type":"result","id":frame["id"],"output":42}),
        )
        .await
        .unwrap();
        assert_eq!(call.await.unwrap().unwrap(), json!(42));
        let activator = host.clone();
        let activation = tokio::spawn(async move { activator.activate(Handlers::new()).await });
        next(&mut peer, "ready").await;
        assert!(!activation.is_finished());
        assert_eq!(
            host.activate(Handlers::new()).await.unwrap_err().code,
            "invalid_input"
        );
        write_frame(&mut peer, &json!({"type":"ready"}))
            .await
            .unwrap();
        activation.await.unwrap().unwrap();
        assert_eq!(
            host.activate(Handlers::new()).await.unwrap_err().code,
            "invalid_input"
        );
        host.close();
    }

    #[tokio::test]
    async fn invalid_staged_bindings_close_without_ready() {
        let definition = json!({"mode":"unary","input":{"type":"null"},"output":{"type":"null"},"error":{"type":"null"}});
        for variant in 0..4 {
            let (host, mut peer) =
                registered_fixture(json!({"version":"1","methods":{"work":definition}})).await;
            let mut handlers = Handlers::new();
            if variant == 1 {
                handlers.stream("work", |_: (), _, _: StreamSender<()>| async { Ok(()) });
            } else if variant > 1 {
                handlers.unary("work", |_: (), _| async { Ok::<_, BusinessError<()>>(()) });
                handlers.unary(
                    if variant == 2 { "extra" } else { "work" },
                    |_: (), _| async { Ok::<_, BusinessError<()>>(()) },
                );
            }
            assert_eq!(
                host.activate(handlers).await.unwrap_err().code,
                "contract_mismatch"
            );
            assert_closed_without_ready(&mut peer).await;
        }
    }

    #[tokio::test]
    async fn failed_or_cancelled_activation_closes_existing_transport() {
        for cancelled in [false, true] {
            let (host, mut peer) = registered_fixture(json!({"version":"1","methods":{}})).await;
            let activator = host.clone();
            let activation = tokio::spawn(async move { activator.activate(Handlers::new()).await });
            next(&mut peer, "ready").await;
            if cancelled {
                activation.abort();
                assert!(activation.await.unwrap_err().is_cancelled());
            } else {
                write_frame(
                    &mut peer,
                    &json!({"type":"error","code":"not_ready","message":"rejected"}),
                )
                .await
                .unwrap();
                assert_eq!(activation.await.unwrap().unwrap_err().code, "not_ready");
            }
            assert_closed_without_ready(&mut peer).await;
            assert!(host.inner.closed.is_cancelled());
        }
    }

    #[tokio::test]
    async fn drain_during_activation_never_restores_readiness() {
        let (host, mut peer) = registered_fixture(json!({"version":"1","methods":{}})).await;
        let activator = host.clone();
        let activation = tokio::spawn(async move { activator.activate(Handlers::new()).await });
        next(&mut peer, "ready").await;
        let draining = host.clone();
        let drain = tokio::spawn(async move { draining.drain(1000).await });
        assert_eq!(activation.await.unwrap().unwrap_err().code, "not_ready");
        let _ = drain.await.unwrap();
        assert!(host.inner.closed.is_cancelled());
        assert_eq!(
            host.activate(Handlers::new()).await.unwrap_err().code,
            "connection_lost"
        );
    }

    #[test]
    fn shared_restricted_schema_fixtures_agree_on_constraints_and_presence() {
        let cases: Value =
            serde_json::from_str(include_str!("../tests/schema-cases.json")).unwrap();
        for case in cases.as_array().unwrap() {
            for value in case["accept"].as_array().unwrap() {
                assert!(
                    protocol::validate_value(&case["schema"], value).is_ok(),
                    "{}: {value}",
                    case["name"]
                );
            }
            for value in case["reject"].as_array().unwrap() {
                assert!(
                    protocol::validate_value(&case["schema"], value).is_err(),
                    "{}: {value}",
                    case["name"]
                );
            }
        }
    }

    #[tokio::test]
    async fn invalid_bindings_fail_before_connecting() {
        let definition = json!({"mode":"unary","input":{"type":"null"},"output":{"type":"null"},"error":{"type":"null"}});
        let contract = json!({"version":"1","methods":{"work":definition}});
        for variant in 0..4 {
            let mut handlers = Handlers::new();
            if variant == 1 {
                handlers.stream("work", |_: (), _, _: StreamSender<()>| async { Ok(()) });
            } else if variant != 0 {
                handlers.unary("work", |_: (), _| async { Ok::<_, BusinessError<()>>(()) });
                handlers.unary(
                    if variant == 2 { "extra" } else { "work" },
                    |_: (), _| async { Ok::<_, BusinessError<()>>(()) },
                );
            }
            let error = RuntimeHost::connect(
                HostOptions {
                    namespace: "demo".into(),
                    service: "test".into(),
                    instance: "one".into(),
                    port: 0,
                    contract: contract.clone(),
                    max_concurrency: 8,
                },
                handlers,
            )
            .await
            .err()
            .unwrap();
            assert_eq!(error.code, "contract_mismatch");
        }
    }

    #[tokio::test]
    async fn failed_startup_drops_socket_without_starting_background_tasks() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connect = tokio::spawn(RuntimeHost::connect(
            HostOptions {
                namespace: "demo".into(),
                service: "test".into(),
                instance: "one".into(),
                port,
                contract: json!({"version":"1","methods":{}}),
                max_concurrency: 8,
            },
            Handlers::new(),
        ));
        let (mut peer, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut peer).await.unwrap()["type"], "hello");
        write_frame(
            &mut peer,
            &json!({"type":"error","code":"contract_mismatch","message":"rejected"}),
        )
        .await
        .unwrap();
        assert!(connect.await.unwrap().is_err());
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn caller_contract_rejects_input_output_and_business_error_bounds() {
        let (host, mut peer) = fixture().await;
        let definition = json!({"input":{"type":"integer","minimum":0},"output":{"type":"integer","maximum":2},"error":{"type":"string","maxLength":2}});
        let error = host
            .call_contract::<_, i64>(&target(), "work", -1, CallOptions::default(), &definition)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_input");
        assert!(host.inner.state().pending.is_empty());
        for reply in [
            json!({"type":"result","output":3}),
            json!({"type":"error","code":"business","message":"failed","details":"long"}),
        ] {
            let host = host.clone();
            let definition = definition.clone();
            let call = tokio::spawn(async move {
                host.call_contract::<_, i64>(
                    &target(),
                    "work",
                    1,
                    CallOptions::default(),
                    &definition,
                )
                .await
            });
            let frame = next(&mut peer, "call").await;
            let mut reply = reply;
            reply["id"] = frame["id"].clone();
            write_frame(&mut peer, &reply).await.unwrap();
            assert_eq!(call.await.unwrap().unwrap_err().code, "invalid_output");
        }
        host.close();
    }

    #[tokio::test]
    async fn schema_and_deserialization_stream_failures_cancel_once_and_terminalize() {
        let (host, mut peer) = fixture().await;
        let definition = json!({"input":{"type":"null"},"output":{"type":"integer","maximum":2},"error":{"type":"null"}});
        for schema_checked in [true, false] {
            let mut stream: ResponseStream<i64> = if schema_checked {
                host.stream_contract(&target(), "work", (), CallOptions::default(), &definition)
                    .unwrap()
            } else {
                host.stream(&target(), "work", (), CallOptions::default())
                    .unwrap()
            };
            let frame = next(&mut peer, "call").await;
            let task = tokio::spawn(async move {
                assert_eq!(stream.next().await.unwrap_err().code, "invalid_output");
                assert!(stream.next().await.unwrap().is_none());
                stream
            });
            next(&mut peer, "credit").await;
            write_frame(&mut peer, &json!({"type":"chunk","id":frame["id"],"output":if schema_checked { json!(3) } else { json!("bad") }})).await.unwrap();
            let stream = task.await.unwrap();
            assert_eq!(next(&mut peer, "cancel").await["id"], frame["id"]);
            assert!(host.inner.state().pending.is_empty());
            drop(stream);
        }
        let mut stream: ResponseStream<i64> = host
            .stream_contract(&target(), "work", (), CallOptions::default(), &definition)
            .unwrap();
        let frame = next(&mut peer, "call").await;
        let task = tokio::spawn(async move {
            assert_eq!(stream.next().await.unwrap_err().code, "invalid_output");
            assert!(stream.next().await.unwrap().is_none());
        });
        next(&mut peer, "credit").await;
        write_frame(&mut peer, &json!({"type":"error","id":frame["id"],"code":"business","message":"failed","details":1})).await.unwrap();
        task.await.unwrap();
        assert!(host.inner.state().pending.is_empty());
        host.close();
    }

    #[tokio::test]
    async fn oversized_values_are_rejected_before_unbounded_json_allocation() {
        let oversized = "\n".repeat(protocol::MAX_FRAME_BYTES);
        let mut writer = BoundedJson { bytes: Vec::new() };
        assert!(serde_json::to_writer(&mut writer, &oversized).is_err());
        assert!(writer.bytes.len() < protocol::MAX_FRAME_BYTES);
        assert!(writer.bytes.capacity() < protocol::MAX_FRAME_BYTES);
        assert_eq!(
            bounded_value(&oversized, "invalid_output")
                .unwrap_err()
                .code,
            "invalid_output"
        );
        let error = RuntimeError::business(&oversized);
        assert_eq!(error.code, "invalid_output");
        assert!(error.details.is_none());
        let (host, _) = fixture().await;
        assert_eq!(
            host.call::<_, Value>(&target(), "work", &oversized, CallOptions::default())
                .await
                .unwrap_err()
                .code,
            "invalid_input"
        );
        assert!(host.inner.state().pending.is_empty());
        // The newline consumes the last byte of the exact transport budget.
        let boundary = "x".repeat(protocol::MAX_FRAME_BYTES - 3);
        assert_eq!(
            bounded_json(&boundary, "invalid_input").unwrap().len(),
            protocol::MAX_FRAME_BYTES - 1
        );
        let frame = bounded_frame(&Value::String(boundary.clone()), "invalid_input").unwrap();
        assert_eq!(frame.len(), protocol::MAX_FRAME_BYTES);
        assert_eq!(frame.capacity(), protocol::MAX_FRAME_BYTES);
        assert_eq!(frame.last(), Some(&b'\n'));
        assert_eq!(
            bounded_json(&(boundary + "x"), "invalid_input")
                .unwrap_err()
                .code,
            "invalid_input"
        );
    }

    #[tokio::test]
    async fn concurrent_and_repeated_drain_share_one_frame_and_deadline() {
        let (host, mut peer) = fixture().await;
        let first = host.clone();
        let first = tokio::spawn(async move { first.drain(1000).await });
        assert_eq!(next(&mut peer, "drain").await["timeout_ms"], 1000);
        let second = host.clone();
        let second = tokio::spawn(async move { second.drain(1).await });
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!second.is_finished());
        write_frame(&mut peer, &json!({"type":"drained"}))
            .await
            .unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        host.drain(1).await.unwrap();
        assert!(timeout(Duration::from_millis(20), next(&mut peer, "drain"))
            .await
            .is_err());
        host.close();

        let (host, mut peer) = fixture().await;
        // Dropping the initial waiter must not reset the first absolute deadline.
        assert!(timeout(Duration::from_millis(10), host.drain(1))
            .await
            .is_err());
        next(&mut peer, "drain").await;
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(
            host.drain(60000).await.unwrap_err().code,
            "deadline_exceeded"
        );
        assert_eq!(
            host.drain(60000).await.unwrap_err().code,
            "deadline_exceeded"
        );
    }

    #[tokio::test]
    async fn full_writer_rejects_new_work_but_failed_cancellation_closes_connection() {
        let (writer, mut receiver) = mpsc::channel(1);
        let host = RuntimeHost {
            inner: Arc::new(Inner {
                writer,
                contract: json!({"version":"1","methods":{}}),
                host_handles: AtomicUsize::new(1),
                closed: Cancellation::new(),
                accepting: AtomicBool::new(true),
                sequence: AtomicU64::new(0),
                admission: Mutex::new(()),
                state: Mutex::new(State {
                    pending: HashMap::new(),
                    invocations: HashMap::new(),
                    executions: 0,
                    drained: None,
                    handlers: None,
                    activation: None,
                }),
            }),
        };
        let (sender, mut result) = oneshot::channel();
        let guard = host
            .start(
                &target(),
                "count",
                json!({}),
                &CallOptions::default(),
                PendingKind::Unary(sender),
            )
            .unwrap();
        assert_eq!(
            host.call::<_, i64>(&target(), "count", json!({}), CallOptions::default())
                .await
                .unwrap_err()
                .code,
            "overloaded"
        );
        assert!(!host.inner.closed.is_cancelled());
        assert_eq!(host.inner.state().pending.len(), 1);
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        receiver.recv().await.unwrap();
        let mut stream = host
            .stream::<_, i64>(&target(), "progress", json!({}), CallOptions::default())
            .unwrap();
        receiver.recv().await.unwrap();
        host.inner.send(json!({"type":"heartbeat"})).unwrap();
        // A decode failure must not forget a route when cancel cannot enqueue.
        assert_eq!(
            stream.decode(json!("bad")).unwrap_err().code,
            "invalid_output"
        );
        assert_eq!(stream.next().await.unwrap(), None);
        assert!(host.inner.state().pending.is_empty());
        assert!(host.inner.closed.is_cancelled());
        assert_eq!(result.await.unwrap().unwrap_err().code, "overloaded");
        receiver.recv().await.unwrap();
        drop(receiver);
        assert_eq!(
            host.inner
                .send(json!({"type":"heartbeat"}))
                .unwrap_err()
                .code,
            "connection_lost"
        );
        drop(guard);
    }

    // Single-frame tests do not cover a suspended next with both branches ready.
    #[tokio::test]
    async fn terminal_errors_win_over_buffered_chunks_and_clear_stream_wait_state() {
        let (host, mut peer) = fixture().await;
        for _ in 0..32 {
            let mut stream = host
                .stream::<_, i64>(&target(), "work", (), CallOptions::default())
                .unwrap();
            let frame = next(&mut peer, "call").await;
            {
                let pending_next = stream.next();
                tokio::pin!(pending_next);
                assert!(timeout(Duration::from_millis(1), &mut pending_next)
                    .await
                    .is_err());
                next(&mut peer, "credit").await;
                let pending = host
                    .inner
                    .state()
                    .pending
                    .remove(frame["id"].as_str().unwrap())
                    .unwrap();
                if let PendingKind::Stream { chunks, .. } = &pending.kind {
                    chunks.try_send(json!("bad")).unwrap();
                }
                finish_pending(pending, Err(RuntimeError::business("failed")));
                assert_eq!(pending_next.await.unwrap_err().code, "business");
            }
            assert!(!stream.waiting);
            assert_eq!(stream.credit.load(Ordering::Acquire), 0);
            assert!(stream.next().await.unwrap().is_none());
        }
        host.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_waits_for_cancelled_handler_future_drop_even_after_disconnect() {
        struct Cleanup {
            dropping: Cancellation,
            release: std::sync::mpsc::Receiver<()>,
            finished: Cancellation,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.dropping.cancel();
                self.release.recv_timeout(Duration::from_secs(2)).unwrap();
                self.finished.cancel();
            }
        }
        let started = Cancellation::new();
        let dropping = Cancellation::new();
        let finished = Cancellation::new();
        let (release, released) = std::sync::mpsc::channel();
        let cleanup = Arc::new(Mutex::new(Some(Cleanup {
            dropping: dropping.clone(),
            release: released,
            finished: finished.clone(),
        })));
        let mut handlers = Handlers::new();
        handlers.unary::<Value, Value, Value, _, _>("work", {
            let started = started.clone();
            move |_, _| {
                let cleanup = cleanup.lock().unwrap().take().unwrap();
                let started = started.clone();
                async move {
                    let _cleanup = cleanup;
                    started.cancel();
                    std::future::pending().await
                }
            }
        });
        let contract = json!({"version":"1","methods":{"work":{"mode":"unary","input":{"type":"object","properties":{},"required":[],"additionalProperties":false},"output":{"type":"null"},"error":{"type":"null"}}}});
        let (host, mut peer) = fixture_with(handlers, contract).await;
        write_frame(&mut peer, &json!({"type":"invoke","id":"1","method":"work","input":{},"timeout_ms":1000,"trace_id":"trace","caller":{"namespace":"demo","service":"worker","instance":"one"}})).await.unwrap();
        timeout(Duration::from_secs(1), started.cancelled())
            .await
            .unwrap();
        write_frame(&mut peer, &json!({"type":"cancel","id":"1"}))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), dropping.cancelled())
            .await
            .unwrap();
        let draining = host.clone();
        let draining = tokio::spawn(async move { draining.drain(1000).await });
        next(&mut peer, "drain").await;
        write_frame(&mut peer, &json!({"type":"drained"}))
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(20), host.drain(1000))
            .await
            .is_err());
        release.send(()).unwrap();
        draining.await.unwrap().unwrap();
        assert!(finished.is_cancelled());
        host.close();
        host.drain(1000).await.unwrap();

        // fail() removes routes eagerly, but a disconnected drain still counts leases.
        let (host, _) = fixture().await;
        let lease = {
            let mut state = host.inner.state();
            state.executions += 1;
            Execution {
                inner: host.inner.clone(),
                id: "local".into(),
                cancellation: Cancellation::new(),
                credits: Arc::new(Semaphore::new(0)),
            }
        };
        host.close();
        assert!(timeout(Duration::from_millis(10), host.drain(1000))
            .await
            .is_err());
        drop(lease);
        host.drain(1).await.unwrap();
    }

    #[tokio::test]
    async fn typed_unary_and_disconnect_fail_without_replay() {
        let (host, mut peer) = fixture().await;
        let caller = host.clone();
        let call = tokio::spawn(async move {
            caller
                .call::<_, i64>(&target(), "count", json!({}), CallOptions::default())
                .await
        });
        let frame = next(&mut peer, "call").await;
        write_frame(
            &mut peer,
            &json!({"type":"result","id":frame["id"],"output":7}),
        )
        .await
        .unwrap();
        assert_eq!(call.await.unwrap().unwrap(), 7);
        let caller = host.clone();
        let pending = tokio::spawn(async move {
            caller
                .call::<_, i64>(&target(), "count", json!({}), CallOptions::default())
                .await
        });
        next(&mut peer, "call").await;
        drop(peer);
        assert_eq!(pending.await.unwrap().unwrap_err().code, "connection_lost");
        assert_eq!(
            host.call::<_, i64>(&target(), "count", json!({}), CallOptions::default())
                .await
                .unwrap_err()
                .code,
            "connection_lost"
        );
        host.close();
    }
    #[tokio::test]
    async fn consumer_demand_and_drop_cancel_stream() {
        let (host, mut peer) = fixture().await;
        let mut stream = host
            .stream::<_, i64>(&target(), "progress", json!({}), CallOptions::default())
            .unwrap();
        let call = next(&mut peer, "call").await;
        let consume = tokio::spawn(async move {
            let item = stream.next().await.unwrap();
            (stream, item)
        });
        let credit = next(&mut peer, "credit").await;
        assert_eq!(credit["credits"], 1);
        write_frame(
            &mut peer,
            &json!({"type":"chunk","id":call["id"],"output":4}),
        )
        .await
        .unwrap();
        let (stream, item) = consume.await.unwrap();
        assert_eq!(item, Some(4));
        drop(stream);
        assert_eq!(next(&mut peer, "cancel").await["id"], call["id"]);
        host.close();
    }
    #[tokio::test]
    async fn explicit_cancellation_and_deadline_terminate_routes() {
        let (host, mut peer) = fixture().await;
        let cancellation = Cancellation::new();
        let options = CallOptions {
            cancellation: Some(cancellation.clone()),
            ..Default::default()
        };
        let caller = host.clone();
        let call = tokio::spawn(async move {
            caller
                .call::<_, i64>(&target(), "count", json!({}), options)
                .await
        });
        let frame = next(&mut peer, "call").await;
        cancellation.cancel();
        assert_eq!(call.await.unwrap().unwrap_err().code, "cancelled");
        assert_eq!(next(&mut peer, "cancel").await["id"], frame["id"]);
        let caller = host.clone();
        let call = tokio::spawn(async move {
            caller
                .call::<_, i64>(
                    &target(),
                    "count",
                    json!({}),
                    CallOptions {
                        timeout_ms: 20,
                        ..Default::default()
                    },
                )
                .await
        });
        let frame = next(&mut peer, "call").await;
        assert_eq!(call.await.unwrap().unwrap_err().code, "deadline_exceeded");
        assert_eq!(next(&mut peer, "cancel").await["id"], frame["id"]);
        host.close();
    }
    #[tokio::test]
    async fn chunk_without_demand_fails_connection() {
        let (host, mut peer) = fixture().await;
        let mut stream = host
            .stream::<_, i64>(&target(), "progress", json!({}), CallOptions::default())
            .unwrap();
        let call = next(&mut peer, "call").await;
        write_frame(
            &mut peer,
            &json!({"type":"chunk","id":call["id"],"output":4}),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(stream.next().await.unwrap_err().code, "protocol_error");
        host.close();
    }
    #[tokio::test]
    async fn dropping_last_host_closes_connection_tasks() {
        let (host, mut peer) = fixture().await;
        drop(host);
        let result = timeout(Duration::from_secs(1), async {
            loop {
                if let Err(error) = read_frame(&mut peer).await {
                    return error;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result.code, "connection_lost");
    }
}
