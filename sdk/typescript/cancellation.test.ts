import { expect, test } from "bun:test";
import { waitForOperation } from "./cancellation";

test("operation waits preserve active errors and clean subscriptions", async () => {
  const controller = new AbortController();
  const failure = new Error("operation failed");
  await expect(waitForOperation(Promise.reject(failure), controller.signal)).rejects.toBe(failure);
  expect(await waitForOperation(Promise.resolve(42), controller.signal)).toBe(42);
  controller.abort(new Error("too late"));
});

for (const alreadyAborted of [false, true]) {
  test(`operation wait consumes late rejection, already aborted: ${alreadyAborted}`, async () => {
    const controller = new AbortController();
    const reason = new Error("cancelled");
    let reject!: (failure: unknown) => void;
    const operation = new Promise<never>((_resolve, fail) => { reject = fail; });
    if (alreadyAborted) controller.abort(reason);
    const waiting = waitForOperation(operation, controller.signal);
    if (!alreadyAborted) controller.abort(reason);
    await expect(waiting).rejects.toBe(reason);
    reject(new Error("late failure"));
    await Bun.sleep(0);
  });
}
