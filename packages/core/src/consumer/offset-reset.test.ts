import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../errors';
import {
  isByDurationReset,
  listOffsetsQueryForReset,
  parseIso8601DurationMs,
  resolveAutoOffsetReset,
  timestampForByDurationReset,
  topicOffsetConfigurationFromSubscribe,
} from './offset-reset';

describe('consumer/offset-reset', () => {
  describe('resolveAutoOffsetReset', () => {
    it('returns latest when fromBeginning is omitted', () => {
      expect(resolveAutoOffsetReset(undefined)).toBe('latest');
      expect(resolveAutoOffsetReset({})).toBe('latest');
      expect(resolveAutoOffsetReset({ fromBeginning: false })).toBe('latest');
    });

    it('returns earliest when fromBeginning is true', () => {
      expect(resolveAutoOffsetReset({ fromBeginning: true })).toBe('earliest');
    });

    it('lets autoOffsetReset win over fromBeginning', () => {
      expect(resolveAutoOffsetReset({ fromBeginning: true, autoOffsetReset: 'none' })).toBe('none');
      expect(resolveAutoOffsetReset({ fromBeginning: false, autoOffsetReset: 'earliest' })).toBe('earliest');
      expect(resolveAutoOffsetReset({ fromBeginning: true, autoOffsetReset: 'latest' })).toBe('latest');
      expect(resolveAutoOffsetReset({ fromBeginning: true, autoOffsetReset: 'by_duration:PT1H' })).toBe(
        'by_duration:PT1H',
      );
    });
  });

  describe('topicOffsetConfigurationFromSubscribe', () => {
    it('stores autoOffsetReset from subscribe options', () => {
      expect(topicOffsetConfigurationFromSubscribe({ autoOffsetReset: 'none' })).toEqual({
        fromBeginning: false,
        autoOffsetReset: 'none',
      });
      expect(topicOffsetConfigurationFromSubscribe({ fromBeginning: true, autoOffsetReset: 'earliest' })).toEqual({
        fromBeginning: true,
        autoOffsetReset: 'earliest',
      });
      expect(topicOffsetConfigurationFromSubscribe({ autoOffsetReset: 'by_duration:P1D' })).toEqual({
        fromBeginning: false,
        autoOffsetReset: 'by_duration:P1D',
      });
    });

    it('keeps latest when fromBeginning is omitted and autoOffsetReset is unset', () => {
      expect(topicOffsetConfigurationFromSubscribe({})).toEqual({ fromBeginning: false });
    });

    it('uses the consumer default when subscribe omits autoOffsetReset', () => {
      expect(topicOffsetConfigurationFromSubscribe({ fromBeginning: true }, 'none')).toEqual({
        fromBeginning: true,
        autoOffsetReset: 'none',
      });
    });
  });

  describe('parseIso8601DurationMs', () => {
    it('parses day, hour, minute, and second components', () => {
      expect(parseIso8601DurationMs('PT1H')).toBe(3_600_000);
      expect(parseIso8601DurationMs('PT30M')).toBe(1_800_000);
      expect(parseIso8601DurationMs('PT15S')).toBe(15_000);
      expect(parseIso8601DurationMs('P1D')).toBe(86_400_000);
      expect(parseIso8601DurationMs('P2DT3H4M5S')).toBe(2 * 86_400_000 + 3 * 3_600_000 + 4 * 60_000 + 5_000);
      expect(parseIso8601DurationMs('PT0.5S')).toBe(500);
    });

    it('honors a leading minus', () => {
      expect(parseIso8601DurationMs('-PT1H')).toBe(-3_600_000);
    });

    it('rejects empty or calendar-period forms', () => {
      expect(() => parseIso8601DurationMs('')).toThrow(KafkaNonRetriableError);
      expect(() => parseIso8601DurationMs('P')).toThrow(KafkaNonRetriableError);
      expect(() => parseIso8601DurationMs('PT')).toThrow(KafkaNonRetriableError);
      expect(() => parseIso8601DurationMs('P1Y')).toThrow(/Invalid ISO-8601 duration/);
      expect(() => parseIso8601DurationMs('P1M')).toThrow(/Invalid ISO-8601 duration/);
      expect(() => parseIso8601DurationMs('1h')).toThrow(/Invalid ISO-8601 duration/);
    });
  });

  describe('listOffsetsQueryForReset', () => {
    it('returns null for none', () => {
      expect(listOffsetsQueryForReset('events', [{ partition: 0 }], 'none')).toBeNull();
    });

    it('maps earliest and latest to fromBeginning', () => {
      expect(listOffsetsQueryForReset('events', [{ partition: 0 }], 'earliest')).toEqual({
        topic: 'events',
        partitions: [{ partition: 0 }],
        fromBeginning: true,
      });
      expect(listOffsetsQueryForReset('events', [{ partition: 0 }], 'latest')).toEqual({
        topic: 'events',
        partitions: [{ partition: 0 }],
        fromBeginning: false,
      });
    });

    it('maps by_duration to now minus the duration', () => {
      expect(isByDurationReset('by_duration:PT1H')).toBe(true);
      expect(listOffsetsQueryForReset('events', [{ partition: 1 }], 'by_duration:PT1H', 10_000_000)).toEqual({
        topic: 'events',
        partitions: [{ partition: 1 }],
        fromTimestamp: timestampForByDurationReset('by_duration:PT1H', 10_000_000),
      });
      expect(timestampForByDurationReset('by_duration:PT1H', 10_000_000)).toBe(10_000_000n - 3_600_000n);
    });
  });
});
