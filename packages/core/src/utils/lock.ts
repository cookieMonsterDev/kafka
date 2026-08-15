import { KafkaJSLockTimeout } from '../errors.js';

export interface LockOptions {
  timeout: number;
  description?: string;
}

export interface AcquireOptions {
  signal?: AbortSignal;
}

export class Lock {
  #locked = false;
  #timeout: number;
  #description: string | undefined;
  #waiting = new Set<() => void>();

  constructor(options: Partial<LockOptions> = {}) {
    const { timeout, description } = options;
    if (typeof timeout !== 'number') {
      throw new TypeError(`'timeout' is not a number, received '${typeof timeout}'`);
    }

    this.#timeout = timeout;
    this.#description = description;
  }

  #timeoutMessage(): string {
    const message = `Timeout while acquiring lock (${this.#waiting.size} waiting locks)`;
    return this.#description ? `${message}: "${this.#description}"` : message;
  }

  async acquire({ signal }: AcquireOptions = {}): Promise<void> {
    if (!this.#locked) {
      this.#locked = true;
      return;
    }

    return new Promise<void>((resolve, reject) => {
      let onAbort: (() => void) | undefined;

      const cleanup = (): void => {
        clearTimeout(timeoutId);
        this.#waiting.delete(tryToAcquire);
        if (onAbort) signal?.removeEventListener('abort', onAbort);
      };

      const tryToAcquire = (): void => {
        if (!this.#locked) {
          this.#locked = true;
          cleanup();
          resolve();
        }
      };

      this.#waiting.add(tryToAcquire);

      const timeoutId = setTimeout(() => {
        const error = new KafkaJSLockTimeout(this.#timeoutMessage());
        cleanup();
        reject(error);
      }, this.#timeout);

      if (signal) {
        onAbort = () => {
          cleanup();
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- propagate the caller's abort reason as-is, matching AbortSignal semantics
          reject(signal.reason);
        };
        signal.addEventListener('abort', onAbort, { once: true });
      }
    });
  }

  async release(): Promise<void> {
    this.#locked = false;
    const [next] = this.#waiting;
    next?.();
  }
}
