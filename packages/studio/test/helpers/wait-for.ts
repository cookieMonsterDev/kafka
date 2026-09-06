export interface WaitForOptions {
  readonly intervalMs?: number;
  readonly timeoutMs?: number;
  readonly message: string;
}

/**
 * Polls `check` until it returns a value other than `false`, then resolves with it — for asserting
 * against state that only becomes visible after it propagates across the broker (metadata for a
 * just-created partition, a just-produced message becoming readable, …). Rejects once `timeoutMs`
 * elapses, naming what it was waiting for rather than a bare timeout.
 */
export async function waitFor<T>(check: () => Promise<T | false>, options: WaitForOptions): Promise<T> {
  const { intervalMs = 250, timeoutMs = 20_000, message } = options;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await check();
    if (result !== false) return result;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${String(timeoutMs)}ms waiting for: ${message}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
