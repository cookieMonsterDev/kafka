/** Fast retry timings for tests. Import explicitly; not selected by environment. */
export const FAST_RETRY_DEFAULTS = Object.freeze({
  maxRetryTime: 1000,
  initialRetryTime: 50,
  factor: 0.02, // randomization factor
  multiplier: 1.5, // exponential factor
  retries: 15, // max retries
});
