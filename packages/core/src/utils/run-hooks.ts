import type { Logger } from '../loggers/index';

/** A single ordered async hook: producer `onSend`/`onAck`, consumer `onConsume`/`onCommit`. */
export type Hook<T> = (event: T) => void | Promise<void>;

/**
 * Runs `hooks` in registration order, awaiting each one before starting the next ("ordered
 * async" hooks, not fan-out). A hook that throws (synchronously or via a rejected promise) is
 * caught and logged here; it never propagates to the caller. This is what keeps a broken
 * user-supplied hook from swallowing (or being confused with) the underlying produce, consume,
 * or commit outcome - the single most important correctness property of the hooks feature.
 */
export async function runHooks<T>(
  hooks: readonly Hook<T>[] | undefined,
  event: T,
  hookName: string,
  logger: Logger,
): Promise<void> {
  if (!hooks || hooks.length === 0) return;

  for (const hook of hooks) {
    try {
      await hook(event);
    } catch (e) {
      const error = e as Error;
      logger.error(`Hook "${hookName}" threw an error`, { error: error.message, stack: error.stack });
    }
  }
}
