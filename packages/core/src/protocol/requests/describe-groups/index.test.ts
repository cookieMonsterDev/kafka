import { describe, expect, it } from 'vitest'
import { DescribeGroups } from './index.js'

describe('protocol/requests/describe-groups', () => {
  it('implements versions 0 through 2', () => {
    expect(DescribeGroups.versions).toEqual([0, 1, 2])
  })
})
