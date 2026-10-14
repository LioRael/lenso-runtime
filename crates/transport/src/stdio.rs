//! Unix stdio uses owned, close-on-exec duplicates and readiness-driven syscalls,
//! not blocking tasks. Duplicates share file status flags with fd 0/1: opening
//! this transport also makes the original streams nonblocking. Stdout is
//! reserved for protocol traffic; diagnostics must use stderr. Do not change
//! the shared flags or concurrently use the original streams.
//!
//! On non-Unix platforms the Tokio fallback does not guarantee cancellation of
//! blocked stdio operations or prompt runtime shutdown. Only Unix is verified.

#[cfg(unix)]
pub(crate) use unix::open;

#[cfg(not(unix))]
pub(crate) type StdioRead = tokio::io::Stdin;
#[cfg(not(unix))]
pub(crate) type StdioWrite = tokio::io::Stdout;

#[cfg(not(unix))]
pub(crate) fn open() -> std::io::Result<(StdioRead, StdioWrite)> {
    Ok((tokio::io::stdin(), tokio::io::stdout()))
}

#[cfg(unix)]
mod unix {
    use std::{
        io,
        os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        pin::Pin,
        task::{ready, Context, Poll},
    };
    use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite, Interest, ReadBuf};

    pub(crate) struct StdioRead(AsyncFd<OwnedFd>);
    pub(crate) struct StdioWrite(Option<AsyncFd<OwnedFd>>);

    pub(crate) fn open() -> io::Result<(StdioRead, StdioWrite)> {
        Ok((
            StdioRead(duplicate(libc::STDIN_FILENO, Interest::READABLE)?),
            StdioWrite(Some(duplicate(libc::STDOUT_FILENO, Interest::WRITABLE)?)),
        ))
    }

    fn duplicate(fd: RawFd, interest: Interest) -> io::Result<AsyncFd<OwnedFd>> {
        // SAFETY: fcntl duplicates the borrowed descriptor without taking ownership.
        let duplicate = retry(|| unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
        // SAFETY: successful F_DUPFD_CLOEXEC returned a fresh descriptor we own.
        let owned = unsafe { OwnedFd::from_raw_fd(duplicate) };
        // SAFETY: owned keeps the descriptor alive for both fcntl calls.
        let flags = retry(|| unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFL) })?;
        retry(|| unsafe {
            libc::fcntl(owned.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK)
        })?;
        // SAFETY: this private OwnedFd owns the duplicate until AsyncFd is
        // dropped. We never replace it or close its descriptor independently.
        Ok(unsafe { AsyncFd::register_with_interest(owned, interest) }?)
    }

    fn retry<T>(mut syscall: impl FnMut() -> T) -> io::Result<T>
    where
        T: Copy + PartialOrd + From<i8>,
    {
        loop {
            let result = syscall();
            if result >= T::from(0) {
                return Ok(result);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    impl AsyncRead for StdioRead {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            loop {
                let mut guard = ready!(self.0.poll_read_ready(cx))?;
                match guard.try_io(|fd| {
                    let bytes = buf.initialize_unfilled();
                    // SAFETY: bytes is initialized, writable and valid for its length;
                    // the owned descriptor is live and has O_NONBLOCK set.
                    retry(|| unsafe {
                        libc::read(fd.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len())
                    })
                }) {
                    Ok(Ok(count)) => {
                        buf.advance(count as usize);
                        return Poll::Ready(Ok(()));
                    }
                    Ok(Err(error)) => return Poll::Ready(Err(error)),
                    Err(_) => continue,
                }
            }
        }
    }

    impl AsyncWrite for StdioWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let Some(fd) = &self.0 else {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stdio writer is shut down",
                )));
            };
            loop {
                let mut guard = ready!(fd.poll_write_ready(cx))?;
                match guard.try_io(|fd| {
                    // SAFETY: buf is readable for its length; the owned descriptor
                    // is live and has O_NONBLOCK set.
                    retry(|| unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) })
                }) {
                    Ok(result) => return Poll::Ready(result.map(|count| count as usize)),
                    Err(_) => continue,
                }
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            // Closing only the duplicate leaves fd 1 open. Explicit SDK close
            // must send protocol unregister rather than rely on stdout EOF.
            self.0.take();
            Poll::Ready(Ok(()))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{future::Future, time::Duration};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        #[test]
        fn halves_are_send_and_unpin() {
            fn read<T: AsyncRead + Unpin + Send>() {}
            fn write<T: AsyncWrite + Unpin + Send>() {}
            read::<StdioRead>();
            write::<StdioWrite>();
        }

        fn pipe() -> (OwnedFd, OwnedFd) {
            let mut fds = [-1; 2];
            // SAFETY: fds has space for the two descriptors returned by pipe.
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            // SAFETY: successful pipe returned two distinct owned descriptors.
            unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
        }

        async fn assert_pending(future: impl Future) {
            tokio::pin!(future);
            std::future::poll_fn(|cx| {
                assert!(future.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }

        #[test]
        fn cancelled_stalled_io_does_not_delay_runtime_drop() {
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let (input, input_peer) = pipe();
                    let (output_peer, output) = pipe();
                    let mut read =
                        StdioRead(duplicate(input.as_raw_fd(), Interest::READABLE).unwrap());
                    let mut write = StdioWrite(Some(
                        duplicate(output.as_raw_fd(), Interest::WRITABLE).unwrap(),
                    ));
                    let block = [0_u8; 4096];
                    loop {
                        // SAFETY: output is live and nonblocking, block is valid.
                        match retry(|| unsafe {
                            libc::write(output.as_raw_fd(), block.as_ptr().cast(), block.len())
                        }) {
                            Ok(_) => {}
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                            Err(error) => panic!("{error}"),
                        }
                    }
                    let mut byte = [0];
                    assert_pending(read.read(&mut byte)).await;
                    assert_pending(write.write_all(b"blocked")).await;
                    drop(read);
                    drop(write);
                    drop((input, input_peer, output, output_peer));
                });
                drop(runtime);
                done_tx.send(()).unwrap();
            });
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            thread.join().unwrap();
        }

        #[tokio::test]
        async fn shutdown_and_reader_drop_close_duplicates_only() {
            let (input, output) = pipe();
            let read = StdioRead(duplicate(input.as_raw_fd(), Interest::READABLE).unwrap());
            let mut write = StdioWrite(Some(
                duplicate(output.as_raw_fd(), Interest::WRITABLE).unwrap(),
            ));
            // SAFETY: both descriptors are alive while inspecting their flags.
            assert_ne!(
                unsafe { libc::fcntl(read.0.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
            assert_ne!(
                unsafe { libc::fcntl(input.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
                0
            );
            assert_ne!(
                unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
                0
            );
            drop(read);
            write.shutdown().await.unwrap();
            write.shutdown().await.unwrap();
            assert!(write.0.is_none());
            assert_eq!(
                write.write(b"x").await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            let mut read = StdioRead(duplicate(input.as_raw_fd(), Interest::READABLE).unwrap());
            let mut write = StdioWrite(Some(
                duplicate(output.as_raw_fd(), Interest::WRITABLE).unwrap(),
            ));
            write.write_all(b"x").await.unwrap();
            let mut byte = [0];
            read.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"x");
            drop(write);
            assert_pending(read.read(&mut byte)).await;
            drop(output);
            assert_eq!(read.read(&mut byte).await.unwrap(), 0);
        }

        #[tokio::test]
        async fn pipe_round_trip_zero_length_and_eof() {
            let (input, output) = pipe();
            let mut read = StdioRead(duplicate(input.as_raw_fd(), Interest::READABLE).unwrap());
            let mut write = StdioWrite(Some(
                duplicate(output.as_raw_fd(), Interest::WRITABLE).unwrap(),
            ));
            assert_eq!(read.read(&mut []).await.unwrap(), 0);
            assert_eq!(write.write(&[]).await.unwrap(), 0);
            write.write_all(b"hello").await.unwrap();
            write.flush().await.unwrap();
            let mut bytes = [0; 5];
            read.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"hello");
            write.shutdown().await.unwrap();
            drop(output);
            assert_eq!(read.read(&mut bytes).await.unwrap(), 0);
        }
    }
}
