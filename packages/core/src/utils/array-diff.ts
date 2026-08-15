/** Returns the elements of `a` that are not present in `b`. */
export function arrayDiff<T>(a: readonly T[], b: readonly T[]): T[] {
  return a.filter((item) => !b.includes(item));
}
