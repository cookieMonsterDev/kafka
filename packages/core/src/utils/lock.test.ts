import { describe, expect, it, vi } from 'vitest'
import { Lock } from './lock.js'
import { sleep } from './wait.js'

describe('utils/Lock', () => {
  it('allows only one resource at a time', async () => {
    const lock = new Lock({ timeout: 1000 })
    const resource = vi.fn()
    const callResource = async () => {
      try {
        await lock.acquire()
        resource(Date.now())
        await sleep(50)
      } finally {
        await lock.release()
      }
    }

    await Promise.all([callResource(), callResource(), callResource()])
    const calls = resource.mock.calls.flat() as number[]
    expect(calls.length).toEqual(3)
    expect(calls[1]! - calls[0]!).toBeGreaterThanOrEqual(45)
    expect(calls[2]! - calls[1]!).toBeGreaterThanOrEqual(45)
  })

  it('throws an error if the lock cannot be acquired within a period', async () => {
    const lock = new Lock({ timeout: 60 })
    const callResource = async () => {
      await lock.acquire()
      await sleep(200)
      // never releases the lock
    }

    await expect(Promise.all([callResource(), callResource(), callResource()])).rejects.toThrow(
      'Timeout while acquiring lock (2 waiting locks)'
    )
  })

  it('throws if constructed without a numeric timeout', () => {
    expect(() => new Lock()).toThrow(`'timeout' is not a number, received 'undefined'`)
  })

  it('allows the lock to be acquired after a timeout', async () => {
    const lock = new Lock({ timeout: 60 })
    const callResource = async () => {
      await lock.acquire()
      try {
        await sleep(100)
      } finally {
        void lock.release()
      }
    }

    await expect(Promise.all([callResource(), callResource(), callResource()])).rejects.toThrow(
      'Timeout while acquiring lock (2 waiting locks)'
    )

    // The first call is still holding the lock (100ms hold vs. the 60ms timeout the two
    // waiters above already failed on); a fresh acquire should wait it out and succeed.
    await expect(callResource()).resolves.toBeUndefined()
  })

  it('includes the configured description in the timeout message', async () => {
    const lock = new Lock({ timeout: 60, description: 'My test mock' })
    const callResource = async () => {
      await lock.acquire()
      await sleep(200)
    }

    await expect(Promise.all([callResource(), callResource(), callResource()])).rejects.toThrow(
      'Timeout while acquiring lock (2 waiting locks): "My test mock"'
    )
  })

  it('rejects with the abort reason when acquire is aborted while waiting', async () => {
    const lock = new Lock({ timeout: 1000 })
    await lock.acquire()

    const controller = new AbortController()
    const waiting = lock.acquire({ signal: controller.signal })
    controller.abort(new Error('gave up waiting'))

    await expect(waiting).rejects.toThrow('gave up waiting')
  })
})
