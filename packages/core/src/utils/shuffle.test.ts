import { describe, expect, it } from 'vitest'
import { shuffle } from './shuffle.js'

describe('utils/shuffle', () => {
  it('shuffles', () => {
    const array = Array.from({ length: 500 }, (_, i) => i)
    const shuffled = shuffle(array)

    expect(shuffled).not.toEqual(array)
    expect([...shuffled].sort((a, b) => a - b)).toEqual(array)
  })

  it('returns the same order for single element arrays', () => {
    expect(shuffle([1])).toEqual([1])
  })

  it('throws if it receives a non-array', () => {
    // @ts-expect-error exercising the runtime guard against non-array input
    expect(() => shuffle(undefined)).toThrow(TypeError)
    // @ts-expect-error exercising the runtime guard against non-array input
    expect(() => shuffle('foo')).toThrow(TypeError)
    // @ts-expect-error exercising the runtime guard against non-array input
    expect(() => shuffle({})).toThrow(TypeError)
  })
})
