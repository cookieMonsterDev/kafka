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

export interface MergeConfigLayersOptions {
  /**
   * Keys merged one level deep instead of replaced atomically. Default **empty** — atomic
   * replacement is the safe default for a value that might be a discriminated union, a
   * `boolean | object`, an array, or a function/object that may hold live resources. A consumer
   * with a key it knows is safe to merge shallowly (core passes `['retry']`, matching what
   * `client.ts`'s producer/consumer already do) opts in explicitly.
   */
  shallowMergeKeys?: Iterable<string>;
}

/**
 * Merges two config layers, `override` taking precedence over `base`. For every key, the highest
 * layer where the value is `!== undefined` wins — `undefined` means "absent", never "unset to
 * falsy": `0`, `false`, and `''` all survive. A key defined nowhere is omitted from the result
 * (not set to `undefined`), so a caller destructuring it with its own default (`x = 5`) still
 * gets that default.
 *
 * Every key is replaced atomically by default; see {@link MergeConfigLayersOptions.shallowMergeKeys}
 * for the one-level-deep alternative (itself subject to the same undefined-is-absent rule on its
 * sub-keys).
 */
export function mergeConfigLayers<T extends Record<string, unknown>>(
  override: Partial<T> | undefined,
  base: Partial<T> | undefined,
  options: MergeConfigLayersOptions = {},
): Partial<T> {
  const shallowMergeKeys = new Set(options.shallowMergeKeys ?? []);
  const result: Record<string, unknown> = {};
  const keys = new Set([...Object.keys(override ?? {}), ...Object.keys(base ?? {})]);

  for (const key of keys) {
    const overrideValue = override?.[key];
    const baseValue = base?.[key];

    if (shallowMergeKeys.has(key)) {
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
