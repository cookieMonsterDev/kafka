/**
 * The exit-code taxonomy every command shares, frozen for the life of the 1.x line. `8`–`69` are
 * reserved for a future command-specific code; nothing in this package assigns one yet.
 */
export const EXIT_CODES = Object.freeze({
  ok: 0,
  operationFailed: 1,
  usage: 2,
  config: 3,
  partialBatch: 4,
  abortedOrUnconfirmed: 5,
  unsupportedByBroker: 6,
  authFailed: 7,
  internalBug: 70,
  aborted: 130,
});

export type ExitCode = (typeof EXIT_CODES)[keyof typeof EXIT_CODES];

/**
 * The exit code for a batch command's per-item results: `ok` when every item succeeded,
 * `operationFailed` when none did, `partialBatch` otherwise.
 */
export function exitForBatchResults<T>(results: readonly T[], isOk: (item: T) => boolean): ExitCode {
  const okCount = results.filter(isOk).length;
  if (okCount === results.length) return EXIT_CODES.ok;
  if (okCount === 0) return EXIT_CODES.operationFailed;
  return EXIT_CODES.partialBatch;
}
