import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../errors';
import {
  checkListOffsetsErrors,
  REPLICA_ID,
  withDefaultTimestamps,
  withDefaultTimestampsAndMaxOffsets,
} from './shared';

describe('protocol/requests/list-offsets/shared', () => {
  it('uses replica id -1 for clients that are not replicas', () => {
    expect(REPLICA_ID).toBe(-1);
  });

  describe('withDefaultTimestamps', () => {
    it('defaults omitted timestamp to latest (-1n) and leader epoch to -1', () => {
      expect(
        withDefaultTimestamps([
          {
            topic: 'orders',
            partitions: [{ partition: 0 }, { partition: 1, timestamp: -2n, currentLeaderEpoch: 4 }],
          },
        ]),
      ).toEqual([
        {
          topic: 'orders',
          partitions: [
            { partition: 0, timestamp: -1n, currentLeaderEpoch: -1 },
            { partition: 1, timestamp: -2n, currentLeaderEpoch: 4 },
          ],
        },
      ]);
    });
  });

  describe('withDefaultTimestampsAndMaxOffsets', () => {
    it('defaults omitted maxNumOffsets to 1', () => {
      expect(
        withDefaultTimestampsAndMaxOffsets([
          {
            topic: 'orders',
            partitions: [{ partition: 0 }, { partition: 1, timestamp: -2n, maxNumOffsets: 5 }],
          },
        ]),
      ).toEqual([
        {
          topic: 'orders',
          partitions: [
            { partition: 0, timestamp: -1n, maxNumOffsets: 1 },
            { partition: 1, timestamp: -2n, maxNumOffsets: 5 },
          ],
        },
      ]);
    });
  });

  describe('checkListOffsetsErrors', () => {
    it('returns when every partition succeeded', () => {
      expect(() => checkListOffsetsErrors({ responses: [{ partitions: [{ errorCode: 0 }] }] })).not.toThrow();
    });

    it('throws the first partition-level protocol error in topic order', () => {
      expect(() =>
        checkListOffsetsErrors({
          responses: [{ partitions: [{ errorCode: 0 }] }, { partitions: [{ errorCode: 1 }, { errorCode: 3 }] }],
        }),
      ).toThrow(KafkaProtocolError);
      expect(() =>
        checkListOffsetsErrors({
          responses: [{ partitions: [{ errorCode: 0 }] }, { partitions: [{ errorCode: 1 }, { errorCode: 3 }] }],
        }),
      ).toThrow('The requested offset is not within the range');
    });
  });
});
