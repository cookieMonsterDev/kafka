import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { OffsetCommit, type OffsetCommitOptions } from './index';
import { requestSchema as requestSchemaV10 } from './v10/request';

const ordersId = Buffer.alloc(16, 1);

function options(topicId?: Buffer): OffsetCommitOptions {
  return {
    groupId: 'g',
    groupGenerationId: 3,
    memberId: 'm',
    topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, offset: 5n }] }],
  };
}

describe('protocol/requests/offset-commit', () => {
  it('implements versions 0 through 10', () => {
    expect(OffsetCommit.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('sends topic ids from v10, with the default leader epoch and metadata', async () => {
    const { request } = OffsetCommit.protocol({ version: 10 })(options(ordersId));
    expect(requestSchemaV10.read(new Decoder((await request.encode()).buffer))).toEqual({
      groupId: 'g',
      groupGenerationId: 3,
      memberId: 'm',
      groupInstanceId: null,
      topics: [{ topicId: ordersId, partitions: [{ partition: 0, offset: 5n, leaderEpoch: -1, metadata: null }] }],
    });
  });

  it('builds a v10 request for a topic without an id, but refuses to encode it', () => {
    // Broker#send builds the negotiated version before preferVersion picks v9 for such a topic.
    const { request } = OffsetCommit.protocol({ version: 10 })(options());
    expect(() => request.encode()).toThrow('OffsetCommit v10 requires a 16-byte topicId for topic orders');
  });

  describe('preferVersion', () => {
    const advertised = { minVersion: 2, maxVersion: 10 };

    it('keeps v10 when every topic has a usable id', () => {
      expect(OffsetCommit.preferVersion?.(options(ordersId), 10, advertised)).toBe(10);
    });

    it('falls back to v9 for a topic without one', () => {
      expect(OffsetCommit.preferVersion?.(options(), 10, advertised)).toBe(9);
      expect(OffsetCommit.preferVersion?.(options(Buffer.alloc(16)), 10, advertised)).toBe(9);
    });

    it('leaves a version below 10 alone', () => {
      expect(OffsetCommit.preferVersion?.(options(), 8, advertised)).toBe(8);
    });
  });
});
