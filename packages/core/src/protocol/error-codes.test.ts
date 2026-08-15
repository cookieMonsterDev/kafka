import { describe, expect, it } from 'vitest'
import { createErrorFromCode, ERROR_CODES, failIfVersionNotSupported, failure, staleMetadata } from './error-codes.js'

describe('protocol/error-codes', () => {
  it('has a unique code per entry', () => {
    const codes = ERROR_CODES.map((e) => e.code)
    expect(new Set(codes).size).toBe(codes.length)
  })

  describe('failure', () => {
    it('is false for the success code (0)', () => {
      expect(failure(0)).toBe(false)
    })

    it('is true for any non-zero code', () => {
      expect(failure(1)).toBe(true)
      expect(failure(-1)).toBe(true)
    })
  })

  describe('createErrorFromCode', () => {
    it('builds a KafkaJSProtocolError from a known code', () => {
      const error = createErrorFromCode(1)
      expect(error.type).toBe('OFFSET_OUT_OF_RANGE')
      expect(error.code).toBe(1)
      expect(error.retriable).toBe(false)
      expect(error.message).toContain('offset')
    })

    it('falls back to a KAFKAJS_UNKNOWN_ERROR_CODE placeholder for unknown codes', () => {
      const error = createErrorFromCode(9999)
      expect(error.type).toBe('KAFKAJS_UNKNOWN_ERROR_CODE')
      expect(error.code).toBe(-99)
      expect(error.retriable).toBe(false)
      expect(error.message).toContain('9999')
    })
  })

  describe('failIfVersionNotSupported', () => {
    it('throws for the unsupported-version code', () => {
      expect(() => failIfVersionNotSupported(35)).toThrow('The version of API is not supported')
    })

    it('does not throw for any other code', () => {
      expect(() => failIfVersionNotSupported(0)).not.toThrow()
      expect(() => failIfVersionNotSupported(1)).not.toThrow()
    })
  })

  describe('staleMetadata', () => {
    it('is true for the three stale-metadata error types', () => {
      expect(staleMetadata({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(true)
      expect(staleMetadata({ type: 'LEADER_NOT_AVAILABLE' })).toBe(true)
      expect(staleMetadata({ type: 'NOT_LEADER_FOR_PARTITION' })).toBe(true)
    })

    it('is false for any other error type', () => {
      expect(staleMetadata({ type: 'OFFSET_OUT_OF_RANGE' })).toBe(false)
      expect(staleMetadata({})).toBe(false)
    })
  })
})
