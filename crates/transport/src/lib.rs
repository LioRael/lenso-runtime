//! Bounded message framing for trusted local connections. Session state belongs to callers.
pub mod diagnostics;
mod stdio;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use std::{future::Future, io, net::IpAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Mutex,
};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Error, Message},
    WebSocketStream,
};

/// Maximum JSON payload, reserving one byte for newline on byte streams.
pub const MAX_PAYLOAD_BYTES: usize = protocol::MAX_FRAME_BYTES - 1;
type Socket = WebSocketStream<TcpStream>;
type Sink = Arc<Mutex<SplitSink<Socket, Message>>>;

/// Independently movable receive and send halves.
pub struct Connection {
    pub reader: Reader,
    pub writer: Writer,
}
/// Receives payloads without transport delimiters.
pub struct Reader {
    inner: ReadKind,
}
enum ReadKind {
    Bytes {
        read: BufReader<Box<dyn AsyncRead + Send + Unpin>>,
        pending: Vec<u8>,
    },
    WebSocket {
        read: SplitStream<Socket>,
        sink: Sink,
    },
}
/// Sends payloads without transport delimiters.
pub struct Writer {
    inner: WriteKind,
}
enum WriteKind {
    Bytes {
        write: Box<dyn AsyncWrite + Send + Unpin>,
        framed: Vec<u8>,
    },
    WebSocket(Sink),
}

impl Connection {
    /// Wrap a TCP stream with newline framing, retaining the v1 TCP socket policy.
    pub fn tcp(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(false)?;
        let (read, write) = stream.into_split();
        Ok(Self::pipes(read, write))
    }
    /// Use service stdin/stdout. Diagnostics must use stderr.
    /// Unix standard-stream descriptors use nonblocking owned duplicates.
    /// Non-Unix currently uses Tokio's blocking wrappers and is not certified.
    pub fn stdio() -> io::Result<Self> {
        let (read, write) = stdio::open()?;
        Ok(Self::pipes(read, write))
    }
    /// Wrap child stdout/stdin or another pair of byte streams.
    pub fn pipes<R, W>(read: R, write: W) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self {
            reader: Reader {
                inner: ReadKind::Bytes {
                    read: BufReader::new(Box::new(read)),
                    pending: Vec::new(),
                },
            },
            writer: Writer {
                inner: WriteKind::Bytes {
                    write: Box::new(write),
                    framed: Vec::new(),
                },
            },
        }
    }
    fn websocket(socket: Socket) -> Self {
        let (sink, read) = socket.split();
        let sink = Arc::new(Mutex::new(sink));
        Self {
            reader: Reader {
                inner: ReadKind::WebSocket {
                    read,
                    sink: sink.clone(),
                },
            },
            writer: Writer {
                inner: WriteKind::WebSocket(sink),
            },
        }
    }
}

impl Reader {
    /// Receive one message; clean EOF returns `None`, partial byte frames are errors.
    /// Byte-frame accumulation survives cancellation of this future.
    pub async fn recv(&mut self) -> io::Result<Option<Vec<u8>>> {
        match &mut self.inner {
            ReadKind::Bytes { read, pending } => loop {
                let available = read.fill_buf().await?;
                if available.is_empty() {
                    return if pending.is_empty() {
                        Ok(None)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unterminated frame",
                        ))
                    };
                }
                let end = available.iter().position(|b| *b == b'\n');
                let count = end.map_or(available.len(), |n| n + 1);
                if pending.len() + count > protocol::MAX_FRAME_BYTES
                    || (end.is_none() && pending.len() + count > MAX_PAYLOAD_BYTES)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "frame exceeds budget",
                    ));
                }
                pending.extend_from_slice(&available[..count]);
                read.consume(count);
                if end.is_some() {
                    pending.pop();
                    return Ok(Some(std::mem::take(pending)));
                }
            },
            ReadKind::WebSocket { read, sink } => loop {
                match read.next().await.transpose().map_err(ws_error)? {
                    Some(Message::Text(text)) => return Ok(Some(text.as_bytes().to_vec())),
                    Some(Message::Binary(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "binary messages unsupported",
                        ))
                    }
                    Some(Message::Ping(_)) => {
                        // Tungstenite queues the automatic pong while reading. Flush through the
                        // same sink as application writes, never a second websocket writer.
                        bounded_ws_write(async { sink.lock().await.flush().await }).await?;
                    }
                    Some(Message::Close(_)) | None => return Ok(None),
                    Some(Message::Pong(_)) | Some(Message::Frame(_)) => (),
                }
            },
        }
    }
}
impl Writer {
    /// Send the existing bounded codec's NDJSON buffer without reframing a copy.
    /// The session codec owns JSON/UTF-8 validation. Only byte transports use
    /// its delimiter on the wire; WebSocket removes it before making one text message.
    pub async fn send_serialized(&mut self, mut frame: Vec<u8>) -> io::Result<()> {
        if frame.last() != Some(&b'\n')
            || frame.len() > protocol::MAX_FRAME_BYTES
            || frame[..frame.len() - 1].contains(&b'\n')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid serialized framing",
            ));
        }
        match &mut self.inner {
            WriteKind::Bytes { write, .. } => tokio::time::timeout(Duration::from_secs(5), async {
                write.write_all(&frame).await?;
                write.flush().await
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "byte transport write stalled"))?,
            WriteKind::WebSocket(sink) => {
                frame.pop();
                let text = String::from_utf8(frame)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                bounded_ws_write(async { sink.lock().await.send(Message::Text(text.into())).await })
                    .await
            }
        }
    }

    /// Send one UTF-8 JSON payload without a trailing newline.
    pub async fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload framing",
            ));
        }
        let text = std::str::from_utf8(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        match &mut self.inner {
            WriteKind::Bytes { write, framed } => {
                if payload.contains(&b'\n') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "newline in byte-stream payload",
                    ));
                }
                // Preserve the original one-write NDJSON path: a separate delimiter
                // write with TCP_NODELAY would add a packet for every message.
                framed.clear();
                framed.extend_from_slice(payload);
                framed.push(b'\n');
                tokio::time::timeout(Duration::from_secs(5), async {
                    write.write_all(framed).await?;
                    write.flush().await
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "byte transport write stalled")
                })?
            }
            WriteKind::WebSocket(sink) => {
                bounded_ws_write(async {
                    sink.lock()
                        .await
                        .send(Message::Text(text.to_owned().into()))
                        .await
                })
                .await
            }
        }
    }
    /// Shut down the send half (or send a WebSocket close frame).
    pub async fn close(&mut self) -> io::Result<()> {
        match &mut self.inner {
            WriteKind::Bytes { write, .. } => {
                tokio::time::timeout(Duration::from_secs(5), write.shutdown())
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "byte transport close stalled")
                    })?
            }
            WriteKind::WebSocket(sink) => {
                bounded_ws_write(async { sink.lock().await.close().await }).await
            }
        }
    }
}
async fn bounded_ws_write<F>(write: F) -> io::Result<()>
where
    F: Future<Output = Result<(), Error>>,
{
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "websocket write stalled"))?
        .map_err(ws_error)
}
fn ws_error(error: Error) -> io::Error {
    match error {
        Error::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}
fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_PAYLOAD_BYTES))
        .max_frame_size(Some(MAX_PAYLOAD_BYTES))
        .write_buffer_size(0)
        .max_write_buffer_size(protocol::MAX_FRAME_BYTES * 2)
}
/// Connect only to literal loopback `ws://` IPs, without DNS resolution.
pub async fn connect_websocket(url: &str) -> io::Result<Connection> {
    if url.contains(['@', '?', '#']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "credentials, query and fragment unsupported",
        ));
    }
    let request = url.into_client_request().map_err(ws_error)?;
    let uri = request.uri();
    if uri.scheme_str() != Some("ws") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only loopback ws supported",
        ));
    }
    let host = uri
        .host()
        .unwrap_or("")
        .trim_start_matches('[')
        .trim_end_matches(']');
    let ip: IpAddr = host
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "loopback IP required"))?;
    if !ip.is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "loopback IP required",
        ));
    }
    let stream = TcpStream::connect((ip, uri.port_u16().unwrap_or(80))).await?;
    stream.set_nodelay(true)?;
    let (socket, _) =
        tokio_tungstenite::client_async_with_config(request, stream, Some(websocket_config()))
            .await
            .map_err(ws_error)?;
    Ok(Connection::websocket(socket))
}
/// Accept a WebSocket upgrade from a loopback peer on a loopback listener.
#[expect(
    clippy::result_large_err,
    reason = "Tungstenite's handshake callback requires an unboxed HTTP error response"
)]
pub async fn accept_websocket(stream: TcpStream) -> io::Result<Connection> {
    if !stream.peer_addr()?.ip().is_loopback() || !stream.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "loopback socket required",
        ));
    }
    stream.set_nodelay(true)?;
    let socket = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if request.headers().contains_key("origin") {
                let mut rejection =
                    tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some(
                        "browser clients unsupported".into(),
                    ));
                *rejection.status_mut() =
                    tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                Err(rejection)
            } else {
                Ok(response)
            }
        },
        Some(websocket_config()),
    )
    .await
    .map_err(ws_error)?;
    Ok(Connection::websocket(socket))
}
