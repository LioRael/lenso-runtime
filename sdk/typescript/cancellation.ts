// Each producer wait owns its abort subscription, not the lifetime of the stream.
export function waitForOperation<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason);
    };
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
    // Keep both handlers attached even after abort to consume late rejections.
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (failure) => {
        signal.removeEventListener("abort", onAbort);
        reject(failure);
      },
    );
  });
}
