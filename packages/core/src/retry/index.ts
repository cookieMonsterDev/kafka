import { KafkaNonRetriableError, KafkaNumberOfRetriesExceeded } from '../errors';
import type { Deadline } from '../utils/deadline';
import { RETRY_DEFAULTS } from './defaults';

/**
 * Exponential backoff used by producer, consumer, and admin requests.
 * @see https://kafka.apache.org/43/configuration/producer-configs/#retries
 */
export interface RetryConfig {
  maxRetryTime: number;
  initialRetryTime: number;
  factor: number;
  multiplier: number;
  retries: number;
}

export type RetryOptions = Partial<RetryConfig>;

export type Bail = (error?: Error) => void;
export type Retryable<T> = (bail: Bail, retryCount: number, retryTime: number) => Promise<T>;
export interface RetryRunOptions {
  /**
   * Stops the retry cycle once expired: a pending backoff is cut short, an attempt already in
   * flight is left to finish, and no further attempt starts. Rejects with the deadline's reason.
   */
  deadline?: Deadline;
}
export type Retrier = <T>(fn: Retryable<T>, options?: RetryRunOptions) => Promise<T>;

function random(min: number, max: number): number {
  return Math.random() * (max - min) + min;
}

function randomFromRetryTime(factor: number, retryTime: number): number {
  const delta = factor * retryTime;
  return Math.ceil(random(retryTime - delta, retryTime + delta));
}

const UNRECOVERABLE_ERROR_NAMES = new Set(['RangeError', 'ReferenceError', 'SyntaxError', 'TypeError']);

function isErrorUnrecoverable(error: Error): boolean {
  return UNRECOVERABLE_ERROR_NAMES.has(error.name);
}

function isErrorRetriable(error: Error & { retriable?: boolean }): boolean {
  return error.retriable !== false && !isErrorUnrecoverable(error);
}

function createRetriable<T>(
  configs: RetryConfig,
  resolve: (value: T) => void,
  reject: (reason: unknown) => void,
  fn: Retryable<T>,
  deadline: Deadline | undefined,
): (retryTime: number, retryCount?: number) => void {
  let aborted = false;
  const { factor, multiplier, maxRetryTime, retries } = configs;

  const stopIfDeadlineExpired = (): boolean => {
    const reason = deadline?.reason;
    if (!reason) return false;
    aborted = true;
    reject(reason);
    return true;
  };

  const bail: Bail = (error) => {
    aborted = true;
    reject(error ?? new Error('Aborted'));
  };

  const calculateExponentialRetryTime = (retryTime: number): number =>
    Math.min(randomFromRetryTime(factor, retryTime) * multiplier, maxRetryTime);

  const retry = (retryTime: number, retryCount = 0): void => {
    if (aborted) return;

    const nextRetryTime = calculateExponentialRetryTime(retryTime);
    const shouldRetry = retryCount < retries;

    const scheduleRetry = (): void => {
      if (!deadline) {
        setTimeout(() => retry(nextRetryTime, retryCount + 1), retryTime);
        return;
      }

      // Only a backoff waits on the deadline, so a cycle that never backs off creates no signal.
      const { signal } = deadline;
      const onAbort = (): void => {
        clearTimeout(timer);
        stopIfDeadlineExpired();
      };
      const timer = setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        retry(nextRetryTime, retryCount + 1);
      }, retryTime);
      signal.addEventListener('abort', onAbort, { once: true });
    };

    if (stopIfDeadlineExpired()) return;

    fn(bail, retryCount, retryTime)
      .then(resolve)
      .catch((e: Error & { retriable?: boolean; cause?: unknown }) => {
        if (stopIfDeadlineExpired()) return;
        if (isErrorRetriable(e)) {
          if (shouldRetry) {
            scheduleRetry();
          } else {
            reject(new KafkaNumberOfRetriesExceeded(e, { retryCount, retryTime }));
          }
        } else {
          reject(new KafkaNonRetriableError(e, { cause: e.cause ?? e }));
        }
      });
  };

  return retry;
}

export function retrier(opts: RetryOptions = {}): Retrier {
  return <T>(fn: Retryable<T>, { deadline }: RetryRunOptions = {}): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      const configs: RetryConfig = { ...RETRY_DEFAULTS, ...opts };
      const start = createRetriable(configs, resolve, reject, fn, deadline);
      start(randomFromRetryTime(configs.factor, configs.initialRetryTime));
    });
}
