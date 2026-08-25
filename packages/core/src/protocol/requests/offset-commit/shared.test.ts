import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../errors';
import {
  checkOffsetCommitErrors,
  RETENTION_TIME,
  withDefaultMetadata,
  withDefaultMetadataAndTimestamp,
} from './shared';

describe('protocol/requests/offset-commit/shared', () => {
  it('exposes the broker default retention sentinel', () => {
    expect(RETENTION_TIME).toBe(-1n);
  });

  describe('withDefaultMetadata', () => {
    it('defaults omitted metadata to null and leaderEpoch to -1', () => {
      expect(
        withDefaultMetadata([
          {
            topic: 'orders',
            partitions: [
              { partition: 0, offset: 10n },
              { partition: 1, offset: 11n, leaderEpoch: 3, metadata: 'note' },
            ],
          },
        ]),
      ).toEqual([
        {
          topic: 'orders',
          partitions: [
            { partition: 0, offset: 10n, leaderEpoch: -1, metadata: null },
            { partition: 1, offset: 11n, leaderEpoch: 3, metadata: 'note' },
          ],
        },
      ]);
    });
  });

  describe('withDefaultMetadataAndTimestamp', () => {
    it('defaults omitted timestamp to now and metadata to null', () => {
      const before = BigInt(Date.now());
      const result = withDefaultMetadataAndTimestamp([
        {
          topic: 'orders',
          partitions: [
            { partition: 0, offset: 1n },
            { partition: 1, offset: 2n, timestamp: 50n, metadata: 'x' },
          ],
        },
      ]);
      const after = BigInt(Date.now());
      const [first, second] = result[0]!.partitions;
      expect(first?.timestamp).toBeGreaterThanOrEqual(before);
      expect(first?.timestamp).toBeLessThanOrEqual(after);
      expect(first?.metadata).toBeNull();
      expect(second).toEqual({ partition: 1, offset: 2n, timestamp: 50n, metadata: 'x' });
    });
  });

  describe('checkOffsetCommitErrors', () => {
    it('returns when every partition succeeded', () => {
      expect(() =>
        checkOffsetCommitErrors({
          responses: [{ partitions: [{ errorCode: 0 }, { errorCode: 0 }] }],
        }),
      ).not.toThrow();
    });

    it('throws the first partition-level protocol error', () => {
      expect(() =>
        checkOffsetCommitErrors({
          responses: [{ partitions: [{ errorCode: 0 }] }, { partitions: [{ errorCode: 0 }, { errorCode: 22 }] }],
        }),
      ).toThrow(KafkaProtocolError);
      expect(() =>
        checkOffsetCommitErrors({
          responses: [{ partitions: [{ errorCode: 22 }] }],
        }),
      ).toThrow('Specified group generation id is not valid');
    });
  });
});
