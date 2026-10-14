//! Bounded, best-effort JSON diagnostics written away from runtime actors.

use serde_json::Value;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::OnceLock;

const CAPACITY: usize = 64;
const MAX_EVENT_BYTES: usize = 4 * 1024;

static DROPPED: AtomicU64 = AtomicU64::new(0);
static SINK: OnceLock<Option<SyncSender<Vec<u8>>>> = OnceLock::new();

/// Queue one JSON diagnostic without waiting for stderr or queue capacity.
///
/// Oversized events and events rejected because the writer is unavailable or
/// the bounded queue is full are discarded and included in [`dropped`].
pub fn emit(event: Value) {
    let Some(sender) = SINK.get_or_init(start_writer).as_ref() else {
        DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Ok(mut frame) = serde_json::to_vec(&event) else {
        DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if frame.len() >= MAX_EVENT_BYTES {
        DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    frame.push(b'\n');
    try_enqueue(sender, frame);
}

fn try_enqueue(sender: &SyncSender<Vec<u8>>, frame: Vec<u8>) {
    if sender.try_send(frame).is_err() {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

/// Number of diagnostics dropped because they could not be retained or written.
pub fn dropped() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

fn start_writer() -> Option<SyncSender<Vec<u8>>> {
    let (sender, receiver) = mpsc::sync_channel(CAPACITY);
    match std::thread::Builder::new()
        .name("lenso-diagnostics".into())
        .spawn(move || write_frames(receiver, io::stderr()))
    {
        Ok(_) => Some(sender),
        Err(_) => None,
    }
}

fn write_frames<W: Write>(receiver: Receiver<Vec<u8>>, mut writer: W) {
    while let Ok(frame) = receiver.recv() {
        if writer.write_all(&frame).is_err() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_channel_rejects_without_waiting_and_counts_drop() {
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.try_send(Vec::new()).unwrap();
        let before = dropped();
        try_enqueue(&sender, Vec::new());
        assert!(dropped() > before);
        assert_eq!(receiver.try_recv().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn writer_failure_is_counted() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("write failed"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let before = dropped();
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.try_send(vec![b'{', b'}', b'\n']).unwrap();
        drop(sender);
        write_frames(receiver, FailingWriter);
        assert!(dropped() > before);
    }
}
