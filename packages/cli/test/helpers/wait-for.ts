export interface WaitForOptions {
  readonly intervalMs?: number;
  readonly timeoutMs?: number;
  readonly message: string;
}

/**
 * Polls `check` until it returns a value other than `false`, then resolves with it — for
 * asserting against state that only becomes true once it has propagated across this suite's
 * multi-broker cluster. A mutating command (`acl add`, `topic delete`, `config set`, …) returning
 * exit code 0 means the broker that handled the request accepted it, not that every broker's own
 * view (what a follow-up `list`/`describe` reads) has caught up yet — a blind vitest-level
 * `retry` re-runs the same fast mutate-then-check sequence from scratch and loses the same race
 * every time if propagation consistently outlasts it, which polling with a real delay does not.
 * Rejects once `timeoutMs` elapses, naming what it was waiting for rather than a bare timeout.
 */
export async function waitFor<T>(check: () => Promise<T | false>, options: WaitForOptions): Promise<T> {
  const { intervalMs = 250, timeoutMs = 10_000, message } = options;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await check();
    if (result !== false) return result;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for: ${message}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
