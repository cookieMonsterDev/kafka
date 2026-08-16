export interface ConnectOptions {
  signal?: AbortSignal;
}

export function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error('Aborted', { cause: signal.reason as unknown });
}

/**
 * Races `promise` against `signal` firing. Doesn't cancel the underlying work (most of this
 * client's retry loops have no abort hook), but the caller sees a rejection as soon as they abort
 * instead of waiting the work out - the same "abort just rejects the promise" contract most Node
 * APIs that accept a bare `AbortSignal` settle for.
 */
export function rejectOnAbort<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(abortError(signal));

  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(abortError(signal));
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}
