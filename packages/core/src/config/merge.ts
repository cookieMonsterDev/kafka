import { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';

/**
 * Core's binding of the generic {@link mergeConfigLayers}: `retry` is shallow-merged one level
 * (matches what `client.ts`'s producer/consumer already do), matching this package's pre-extraction
 * behaviour exactly. Every other key — `sasl`, `ssl`, `brokers`, `metrics`, `socketFactory`,
 * `logCreator`, and everything else — is replaced atomically; see {@link mergeConfigLayers} for why.
 */
export function mergeKafkaConfigLayers<T extends Record<string, unknown>>(
  override: Partial<T> | undefined,
  base: Partial<T> | undefined,
): Partial<T> {
  return mergeConfigLayers(override, base, { shallowMergeKeys: ['retry'] });
}
