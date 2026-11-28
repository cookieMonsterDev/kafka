import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { TxnOffsetCommit, type TxnOffsetCommitOptions } from './index';
import { requestSchema as requestSchemaV6 } from './v6/request';

const ordersId = Buffer.alloc(16, 1);

const options = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 1001n,
  producerEpoch: 0,
  topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n, metadata: null }] }],
};

function withTopicId(topicId: Buffer | undefined): TxnOffsetCommitOptions {
  return { ...options, topics: options.topics.map((topic) => ({ ...topic, topicId })) };
}

describe('protocol/requests/txn-offset-commit', () => {
  it('implements versions 0 through 6', () => {
    expect(TxnOffsetCommit.versions).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it('sends v5 only for a transaction of transaction version 2', () => {
    const advertised = { minVersion: 0, maxVersion: 5 };

    expect(TxnOffsetCommit.preferVersion?.({ ...options, isTransactionV2Enabled: true }, 5, advertised)).toBe(5);
    expect(TxnOffsetCommit.preferVersion?.({ ...options, isTransactionV2Enabled: false }, 5, advertised)).toBe(4);
    expect(TxnOffsetCommit.preferVersion?.(options, 5, advertised)).toBe(4);
    expect(TxnOffsetCommit.preferVersion?.(options, 3, { minVersion: 0, maxVersion: 3 })).toBe(3);
  });

  describe('preferVersion with topic ids', () => {
    const advertised = { minVersion: 0, maxVersion: 6 };

    it('keeps v6 under transaction version 2 when every topic has a usable id', () => {
      expect(
        TxnOffsetCommit.preferVersion?.({ ...withTopicId(ordersId), isTransactionV2Enabled: true }, 6, advertised),
      ).toBe(6);
    });

    it('falls back to v5 for a topic without one', () => {
      for (const topicId of [undefined, Buffer.alloc(16)]) {
        expect(
          TxnOffsetCommit.preferVersion?.({ ...withTopicId(topicId), isTransactionV2Enabled: true }, 6, advertised),
        ).toBe(5);
      }
    });

    it('stays on v4 without transaction version 2, even with topic ids', () => {
      expect(TxnOffsetCommit.preferVersion?.(withTopicId(ordersId), 6, advertised)).toBe(4);
    });
  });

  it('sends the same body from v3 through v5, ignoring topic ids', async () => {
    const encoded = await Promise.all(
      [3, 4, 5].map(
        async (version) => (await TxnOffsetCommit.protocol({ version })(withTopicId(ordersId)).request.encode()).buffer,
      ),
    );
    const withoutIds = (await TxnOffsetCommit.protocol({ version: 3 })(options).request.encode()).buffer;
    expect(encoded[0]).toEqual(withoutIds);
    expect(encoded[1]).toEqual(encoded[0]);
    expect(encoded[2]).toEqual(encoded[0]);
  });

  it('sends topic ids from v6, with the default group member fields and leader epoch', async () => {
    const { request } = TxnOffsetCommit.protocol({ version: 6 })(withTopicId(ordersId));
    expect(requestSchemaV6.read(new Decoder((await request.encode()).buffer))).toEqual({
      transactionalId: 'txn-1',
      groupId: 'g1',
      producerId: 1001n,
      producerEpoch: 0,
      generationId: -1,
      memberId: '',
      groupInstanceId: null,
      topics: [{ topicId: ordersId, partitions: [{ partition: 0, offset: 5n, leaderEpoch: -1, metadata: null }] }],
    });
  });

  it('builds a v6 request for a topic without an id, but refuses to encode it', () => {
    // Broker#send builds the negotiated version before preferVersion picks v5 for such a topic.
    const { request } = TxnOffsetCommit.protocol({ version: 6 })(options);
    expect(() => request.encode()).toThrow('TxnOffsetCommit v6 requires a 16-byte topicId for topic orders');
  });
});
