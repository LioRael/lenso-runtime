use std::io;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let listen = match args.as_slice() {
        [] => "127.0.0.1:7331",
        [flag, value] if flag == "--listen" => value.as_str(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: lenso-runtime [--listen 127.0.0.1:7331]",
            ))
        }
    };
    let address = kernel::loopback_address(listen)?;
    let listener = TcpListener::bind(address).await?;
    eprintln!(
        "{}",
        serde_json::json!({"event":"runtime_listening","address":listener.local_addr()?.to_string()})
    );
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    kernel::serve(listener, async {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => (),
            _ = terminate.recv() => (),
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
