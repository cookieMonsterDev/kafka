/**
 * Keys merged one level deep (matches what `client.ts`'s producer/consumer already do for
 * `retry`). Every other key is replaced atomically — including `sasl`, `ssl`, `brokers`,
 * `metrics`, `socketFactory`, and `logCreator`, none of which are safe to merge field-by-field
 * (a discriminated union, a `boolean | object`, an array, or a function/object that may hold
 * live resources).
 */
const SHALLOW_MERGE_KEYS: ReadonlySet<string> = new Set(['retry']);

function mergeShallowObjects(
  low: Record<string, unknown> | undefined,
  high: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(low ?? {})) {
    if (value !== undefined) result[key] = value;
  }
  for (const [key, value] of Object.entries(high ?? {})) {
    if (value !== undefined) result[key] = value;
  }
  return result;
}

/**
 * Merges two config layers, `override` taking precedence over `base`. For every key, the highest
 * layer where the value is `!== undefined` wins — `undefined` means "absent", never "unset to
 * falsy": `0`, `false`, and `''` all survive. A key defined nowhere is omitted from the result
 * (not set to `undefined`), so a caller destructuring it with its own default (`x = 5`) still
 * gets that default.
 *
 * `retry` is shallow-merged one level (itself subject to the same undefined-is-absent rule on its
 * sub-keys); every other key is replaced atomically — see {@link SHALLOW_MERGE_KEYS}.
 */
export function mergeConfigLayers<T extends Record<string, unknown>>(
  override: Partial<T> | undefined,
  base: Partial<T> | undefined,
): Partial<T> {
  const result: Record<string, unknown> = {};
  const keys = new Set([...Object.keys(override ?? {}), ...Object.keys(base ?? {})]);

  for (const key of keys) {
    const overrideValue = override?.[key];
    const baseValue = base?.[key];

    if (SHALLOW_MERGE_KEYS.has(key)) {
      const merged = mergeShallowObjects(
        baseValue as Record<string, unknown> | undefined,
        overrideValue as Record<string, unknown> | undefined,
      );
      if (Object.keys(merged).length > 0) {
        result[key] = merged;
      }
      continue;
    }

    if (overrideValue !== undefined) {
      result[key] = overrideValue;
    } else if (baseValue !== undefined) {
      result[key] = baseValue;
    }
  }

  return result as Partial<T>;
}
