/**
 * kafkajs auto-selected this via `NODE_ENV=test` (see PLAN.md §5 Phase 1). Here the test
 * harness (test/helpers) imports this explicitly and passes it as a `retry` override instead,
 * so retry timing is never a hidden function of an environment variable.
 */
export const FAST_RETRY_DEFAULTS = {
  maxRetryTime: 1000,
  initialRetryTime: 50,
  factor: 0.02, // randomization factor
  multiplier: 1.5, // exponential factor
  retries: 15, // max retries
} as const
