/** Wraps `fn` so that only the first call actually invokes it; later calls are no-ops. */
export function once<Args extends unknown[], R>(fn: (...args: Args) => R): (...args: Args) => R | undefined {
  let called = false;

  return (...args: Args): R | undefined => {
    if (!called) {
      called = true;
      return fn(...args);
    }
    return undefined;
  };
}
