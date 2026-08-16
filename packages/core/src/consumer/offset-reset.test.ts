import { describe, expect, it } from 'vitest';
import { resolveAutoOffsetReset, topicOffsetConfigurationFromSubscribe } from './offset-reset';

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
});
