use futures_util::{SinkExt, StreamExt};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;
use transport::{Connection, MAX_PAYLOAD_BYTES};

#[tokio::test]
async fn byte_stream_budget_delimiters_eof_and_cancelled_receive() {
    let (mut peer, stream) = tokio::io::duplex(1024);
    let (read, write) = tokio::io::split(stream);
    let mut connection = Connection::pipes(read, write);
    peer.write_all(b"{").await.unwrap();
    assert!(timeout(Duration::from_millis(10), connection.reader.recv())
        .await
        .is_err());
    peer.write_all(b"}\n{}\n").await.unwrap();
    assert_eq!(
        connection.reader.recv().await.unwrap(),
        Some(b"{}".to_vec())
    );
    assert_eq!(
        connection.reader.recv().await.unwrap(),
        Some(b"{}".to_vec())
    );
    peer.shutdown().await.unwrap();
    assert!(connection.reader.recv().await.unwrap().is_none());
}

#[tokio::test]
async fn byte_stream_accepts_exact_budget_and_rejects_one_more() {
    for (length, accepted) in [(MAX_PAYLOAD_BYTES, true), (MAX_PAYLOAD_BYTES + 1, false)] {
        let (mut peer, stream) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(stream);
        let mut connection = Connection::pipes(read, write);
        let sender = tokio::spawn(async move {
            let _ = peer.write_all(&vec![b'x'; length]).await;
            let _ = peer.write_all(b"\n").await;
        });
        let result = connection.reader.recv().await;
        if accepted {
            assert_eq!(result.unwrap().unwrap().len(), length);
        } else {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        }
        drop(connection);
        sender.await.unwrap();
    }
}

#[tokio::test]
async fn partial_eof_and_outbound_budget_are_errors() {
    let mut connection = Connection::pipes(&b"{"[..], tokio::io::sink());
    assert_eq!(
        connection.reader.recv().await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    connection
        .writer
        .send(&vec![b'x'; MAX_PAYLOAD_BYTES])
        .await
        .unwrap();
    assert_eq!(
        connection
            .writer
            .send(&vec![b'x'; MAX_PAYLOAD_BYTES + 1])
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(connection.writer.send(b"{}\n").await.is_err());
}

async fn websocket_pair() -> (
    Connection,
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let address = listener.local_addr().unwrap();
    let connect = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio_tungstenite::client_async(url, stream)
            .await
            .unwrap()
            .0
    });
    let connection = transport::accept_websocket(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    (connection, connect.await.unwrap())
}

#[tokio::test]
async fn websocket_pong_is_flushed_without_application_writes_and_binary_is_rejected() {
    let (connection, mut peer) = websocket_pair().await;
    let mut reader = connection.reader;
    let receive = tokio::spawn(async move { reader.recv().await });
    peer.send(Message::Ping(b"ping".to_vec().into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(b"ping".to_vec().into())
    );
    peer.send(Message::Binary(vec![1].into())).await.unwrap();
    assert_eq!(
        receive.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn websocket_exact_budget_and_oversized_messages() {
    let (mut connection, mut peer) = websocket_pair().await;
    let pretty = b"{\n\"type\":\"heartbeat\"\n}";
    connection.writer.send(pretty).await.unwrap();
    assert_eq!(
        peer.next().await.unwrap().unwrap(),
        Message::Text(std::str::from_utf8(pretty).unwrap().into())
    );
    peer.send(Message::Text("x".repeat(MAX_PAYLOAD_BYTES).into()))
        .await
        .unwrap();
    assert_eq!(
        connection.reader.recv().await.unwrap().unwrap().len(),
        MAX_PAYLOAD_BYTES
    );
    peer.send(Message::Text("x".repeat(MAX_PAYLOAD_BYTES + 1).into()))
        .await
        .unwrap();
    assert_eq!(
        connection.reader.recv().await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn serialized_codec_buffers_use_one_delimiter_only_on_byte_transports() {
    let (mut peer, stream) = tokio::io::duplex(1024);
    let (read, write) = tokio::io::split(stream);
    let mut connection = Connection::pipes(read, write);
    connection
        .writer
        .send_serialized(b"{}\n".to_vec())
        .await
        .unwrap();
    let mut bytes = [0; 3];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"{}\n");
    for bad in [
        Vec::new(),
        b"{}".to_vec(),
        b"{}\n{}\n".to_vec(),
        vec![b'x'; MAX_PAYLOAD_BYTES + 2],
    ] {
        assert!(connection.writer.send_serialized(bad).await.is_err());
    }
    let (mut connection, mut peer) = websocket_pair().await;
    connection
        .writer
        .send_serialized(b"{}\n".to_vec())
        .await
        .unwrap();
    assert_eq!(
        peer.next().await.unwrap().unwrap(),
        Message::Text("{}".into())
    );
}

#[tokio::test]
async fn websocket_remote_tls_and_dns_names_are_not_enabled() {
    for url in [
        "wss://127.0.0.1/",
        "ws://192.0.2.1/",
        "ws://example.com/",
        "ws://user:secret@127.0.0.1/",
        "ws://127.0.0.1/?token=secret",
        "ws://127.0.0.1/#secret",
    ] {
        assert!(transport::connect_websocket(url).await.is_err(), "{url}");
    }
}

#[tokio::test]
async fn websocket_browser_origin_is_rejected() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move {
        let mut request = format!("ws://{address}/").into_client_request().unwrap();
        request
            .headers_mut()
            .insert("origin", "http://localhost".parse().unwrap());
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        assert!(tokio_tungstenite::client_async(request, stream)
            .await
            .is_err());
    });
    assert!(
        transport::accept_websocket(listener.accept().await.unwrap().0)
            .await
            .is_err()
    );
    client.await.unwrap();
}
