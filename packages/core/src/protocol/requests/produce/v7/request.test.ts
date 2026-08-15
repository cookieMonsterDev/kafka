import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index.js';
import v7RequestFixture from '../fixtures/v7-request.json' with { type: 'json' };
import { produceRequestV7 } from './request.js';

const messages = Array.from({ length: 10 }, (_, i) => ({
  key: `key-${i}`,
  value: `value-${i}`,
  timestamp: 1509928155660,
  headers: {
    [`header-a${i}`]: `header-value-a${i}`,
    [`header-b${i}`]: `header-value-b${i}`,
    [`header-c${i}`]: `header-value-c${i}`,
  },
}));

const args = {
  acks: -1,
  timeout: 30000,
  transactionalId: null,
  producerId: 4004n,
  producerEpoch: 0,
  compression: COMPRESSION_TYPES.None,
  topicData: [
    {
      topic: 'test-topic-923030b997a626c23158-517-bdaf87ff-6ab3-4ba6-ac23-ad463d5230cd',
      partitions: [{ partition: 0, firstSequence: 0, messages }],
    },
  ],
};

describe('protocol/requests/produce/v7/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV7({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes a request matching a real kafkajs fixture', async () => {
    const encoder = await produceRequestV7(args).encode();
    expect(encoder.buffer).toEqual(Buffer.from(v7RequestFixture.data));
  });
});
