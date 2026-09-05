/**
 * Matched by `.name`, not `instanceof`: several errors this package checks for (`KafkaConfigError`,
 * `KafkaServerDoesNotSupportApiKey`, …) are thrown by `@cookiemonsterdev/kafka-core` or
 * `@cookiemonsterdev/kafka-config`, separate packages — if this workspace ever ends up with two
 * installed copies of one, its classes are distinct objects even though the errors behave
 * identically. Errors this package defines itself are matched with `instanceof` instead, since
 * that risk doesn't apply to them.
 */
export function hasErrorName(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}
