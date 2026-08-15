export function seq<T = number>(count: number, callback: (index: number) => T = (index) => index as unknown as T): T[] {
  return Array.from({ length: count }, (_, index) => callback(index))
}
