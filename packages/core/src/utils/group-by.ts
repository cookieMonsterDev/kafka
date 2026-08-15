/** Groups `array` by the (possibly async) key returned by `groupFn`, preserving insertion order. */
export async function groupBy<T, K>(array: readonly T[], groupFn: (item: T) => K | Promise<K>): Promise<Map<K, T[]>> {
  const result = new Map<K, T[]>()

  for (const item of array) {
    const group = await groupFn(item)
    const existing = result.get(group)
    if (existing) {
      existing.push(item)
    } else {
      result.set(group, [item])
    }
  }

  return result
}
