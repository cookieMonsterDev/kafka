import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../errors';
import {
  checkOffsetForLeaderEpochErrors,
  REPLICA_ID,
  UNKNOWN_LEADER_EPOCH,
  withCurrentLeaderEpochs,
  withoutCurrentLeaderEpoch,
} from './shared';

describe('protocol/requests/offset-for-leader-epoch/shared', () => {
  it('uses replica id -1 and unknown leader epoch -1', () => {
    expect(REPLICA_ID).toBe(-1);
    expect(UNKNOWN_LEADER_EPOCH).toBe(-1);
  });

  describe('withCurrentLeaderEpochs', () => {
    it('defaults omitted currentLeaderEpoch to unknown', () => {
      expect(
        withCurrentLeaderEpochs([
          {
            topic: 'orders',
            partitions: [
              { partition: 0, leaderEpoch: 5 },
              { partition: 1, currentLeaderEpoch: 2, leaderEpoch: 6 },
            ],
          },
        ]),
      ).toEqual([
        {
          topic: 'orders',
          partitions: [
            { partition: 0, currentLeaderEpoch: -1, leaderEpoch: 5 },
            { partition: 1, currentLeaderEpoch: 2, leaderEpoch: 6 },
          ],
        },
      ]);
    });
  });

  describe('withoutCurrentLeaderEpoch', () => {
    it('drops currentLeaderEpoch for the v0 request', () => {
      expect(
        withoutCurrentLeaderEpoch([
          { topic: 'orders', partitions: [{ partition: 0, currentLeaderEpoch: 2, leaderEpoch: 5 }] },
        ]),
      ).toEqual([{ topic: 'orders', partitions: [{ partition: 0, leaderEpoch: 5 }] }]);
    });
  });

  describe('checkOffsetForLeaderEpochErrors', () => {
    it('returns when every partition succeeded', () => {
      expect(() =>
        checkOffsetForLeaderEpochErrors({
          topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 0 }] }],
        }),
      ).not.toThrow();
    });

    it('throws the first partition-level error with topic and partition extras', () => {
      const failing = () =>
        checkOffsetForLeaderEpochErrors({
          topics: [
            { topic: 'orders', partitions: [{ errorCode: 0, partition: 0 }] },
            { topic: 'payments', partitions: [{ errorCode: 74, partition: 3 }] },
          ],
        });
      expect(failing).toThrow(KafkaProtocolError);
      expect(failing).toThrow('topic: payments');
      expect(failing).toThrow('partition: 3');
    });
  });
});
