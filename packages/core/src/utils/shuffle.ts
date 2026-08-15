/** Returns a shuffled copy of `array` (Fisher-Yates); `array` itself is left untouched. */
export function shuffle<T>(array: readonly T[]): T[] {
  if (!Array.isArray(array)) {
    throw new TypeError("'array' is not an array")
  }

  if (array.length < 2) {
    return array.slice() as T[]
  }

  const copy = array.slice() as T[]

  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    const temp = copy[i] as T
    copy[i] = copy[j] as T
    copy[j] = temp
  }

  return copy
}
