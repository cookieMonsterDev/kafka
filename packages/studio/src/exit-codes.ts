/** Exit-code taxonomy, kept in step with `@cookiemonsterdev/kafka-cli`'s own constants. */
export const EXIT_CODES = Object.freeze({
  ok: 0,
  usage: 2,
  aborted: 130,
  internalBug: 70,
});

export type ExitCode = (typeof EXIT_CODES)[keyof typeof EXIT_CODES];

/** `SIGTERM` gets its own conventional code (128 + 15); everything else aborts as `SIGINT` does. */
export function exitCodeForSignal(reason: unknown): number {
  return reason === 'SIGTERM' ? 143 : EXIT_CODES.aborted;
}
