/** Returns the elements of `a` that are not present in `b` (`O(n+m)` via a Set). */
export function arrayDiff<T>(a: readonly T[], b: readonly T[]): T[] {
  const excluded = new Set(b);
  return a.filter((item) => !excluded.has(item));
}
