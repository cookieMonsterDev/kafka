import { KafkaJSNonRetriableError, KafkaJSNumberOfRetriesExceeded } from '../errors.js';
import { RETRY_DEFAULTS } from './defaults.js';

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
export type Retrier = <T>(fn: Retryable<T>) => Promise<T>;

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
): (retryTime: number, retryCount?: number) => void {
  let aborted = false;
  const { factor, multiplier, maxRetryTime, retries } = configs;

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
      setTimeout(() => retry(nextRetryTime, retryCount + 1), retryTime);
    };

    fn(bail, retryCount, retryTime)
      .then(resolve)
      .catch((e: Error & { retriable?: boolean; cause?: unknown }) => {
        if (isErrorRetriable(e)) {
          if (shouldRetry) {
            scheduleRetry();
          } else {
            reject(new KafkaJSNumberOfRetriesExceeded(e, { retryCount, retryTime }));
          }
        } else {
          reject(new KafkaJSNonRetriableError(e, { cause: e.cause ?? e }));
        }
      });
  };

  return retry;
}

export function retrier(opts: RetryOptions = {}): Retrier {
  return <T>(fn: Retryable<T>): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      const configs: RetryConfig = { ...RETRY_DEFAULTS, ...opts };
      const start = createRetriable(configs, resolve, reject, fn);
      start(randomFromRetryTime(configs.factor, configs.initialRetryTime));
    });
}
