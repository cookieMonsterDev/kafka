/**
 * Wraps a promise-returning function so concurrent calls share the one in-flight promise
 * instead of triggering the underlying operation multiple times in parallel.
 */
export function sharedPromiseTo<Args extends unknown[], R>(
  asyncFunction: (...args: Args) => Promise<R>
): (...args: Args) => Promise<R> {
  let promise: Promise<R> | null = null

  return (...args: Args): Promise<R> => {
    if (promise == null) {
      const current = asyncFunction(...args)
      promise = current.then(
        (value) => {
          promise = null
          return value
        },
        (error) => {
          promise = null
          throw error
        }
      )
    }
    return promise
  }
}
