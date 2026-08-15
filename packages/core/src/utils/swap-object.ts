/** Swaps keys with values. Assumes `obj`'s values are unique. */
export function swapObject<K extends PropertyKey, V extends PropertyKey>(obj: Record<K, V>): Record<V, K> {
  const result = {} as Record<V, K>

  for (const key of Object.keys(obj) as K[]) {
    result[obj[key]] = key
  }

  return result
}
