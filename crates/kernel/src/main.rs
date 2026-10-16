use std::io;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--version"] {
        println!("lenso-runtime {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let mut listen = "127.0.0.1:7331";
    let mut websocket = None;
    let mut processes = None;
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing option value"))?;
        match flag.as_str() {
            "--listen" => listen = value,
            "--websocket" => websocket = Some(value),
            "--processes" => processes = Some(value),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "usage: lenso-runtime [--listen loopback:port] [--websocket loopback:port] [--processes config.json]")),
        }
    }
    let configs = match processes {
        Some(path) => {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process configuration exceeds 1 MiB",
                ));
            }
            serde_json::from_slice::<Vec<kernel::process::ProcessConfig>>(&bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        }
        None => Vec::new(),
    };
    let address = kernel::loopback_address(listen)?;
    let listener = TcpListener::bind(address).await?;
    eprintln!(
        "{}",
        serde_json::json!({"event":"runtime_listening","address":listener.local_addr()?.to_string()})
    );
    let websocket = match websocket {
        Some(address) => {
            let listener = TcpListener::bind(kernel::loopback_address(address)?).await?;
            eprintln!(
                "{}",
                serde_json::json!({"event":"runtime_websocket_listening","address":listener.local_addr()?.to_string()})
            );
            Some(listener)
        }
        None => None,
    };
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    kernel::serve_with_transports(listener, websocket, configs, async {
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
