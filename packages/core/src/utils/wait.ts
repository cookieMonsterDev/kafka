import { setTimeout as delay } from 'node:timers/promises'
import { KafkaJSTimeout } from '../errors.js'

export interface SleepOptions {
  signal?: AbortSignal
}

export function sleep(ms: number, options: SleepOptions = {}): Promise<void> {
  return delay(ms, undefined, options)
}

export interface WaitForOptions {
  delay?: number
  maxWait?: number
  timeoutMessage?: string
  ignoreTimeout?: boolean
  signal?: AbortSignal
}

/**
 * Polls `fn` every `delay` ms until it returns a truthy value, then resolves with it.
 * Rejects with `KafkaJSTimeout` after `maxWait` ms, or with `signal`'s abort reason.
 */
export function waitFor<T>(fn: (elapsed: number) => T | Promise<T>, options: WaitForOptions = {}): Promise<T> {
  const { delay: interval = 50, maxWait = 10_000, timeoutMessage = 'Timeout', ignoreTimeout = false, signal } = options

  return new Promise<T>((resolve, reject) => {
    let settled = false
    let timeoutId: NodeJS.Timeout | undefined
    let elapsed = 0

    const settle = (action: () => void): void => {
      if (settled) return
      settled = true
      clearTimeout(timeoutId)
      action()
    }

    const check = (): void => {
      elapsed += interval
      if (settled) return

      void (async () => {
        try {
          await sleep(interval)
          if (settled) return

          const result = await fn(elapsed)
          if (result) {
            settle(() => resolve(result))
          } else {
            check()
          }
        } catch (error) {
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward fn's/sleep's rejection reason as-is
          settle(() => reject(error))
        }
      })()
    }

    check()

    if (!ignoreTimeout) {
      timeoutId = setTimeout(() => {
        settle(() => reject(new KafkaJSTimeout(timeoutMessage)))
      }, maxWait)
    }

    signal?.addEventListener(
      'abort',
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- propagate the caller's abort reason as-is, matching AbortSignal semantics
      () => settle(() => reject(signal.reason)),
      { once: true }
    )
  })
}
