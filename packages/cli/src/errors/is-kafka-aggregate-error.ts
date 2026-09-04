/**
 * Matched by `.name`, never `instanceof` — a config file's core might not be the same installed
 * copy as the CLI's own, so a class identity check could silently miss it. A batch admin call
 * (`alterPartitionReassignments`, `updateFeatures`, …) either resolves on full success or throws
 * one `KafkaAggregateError` wrapping one item-specific error per failed entry.
 */
export function isKafkaAggregateError(
  error: unknown,
): error is { readonly name: string; readonly errors: readonly unknown[] } {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'KafkaAggregateError' &&
    Array.isArray((error as { errors?: unknown }).errors)
  );
}
