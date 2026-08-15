import { describe, expect, it } from 'vitest'
import { uniq } from './uniq.js'

describe('utils/uniq', () => {
  it('removes duplicate entries, preserving first-seen order', () => {
    expect(uniq([1, 2, 2, 3, 1])).toEqual([1, 2, 3])
  })

  it('returns an empty array for an empty input', () => {
    expect(uniq([])).toEqual([])
  })
})
