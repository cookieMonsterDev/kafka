import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index.js';
import v6RequestFixture from '../fixtures/v6-request.json' with { type: 'json' };
import { produceRequestV6 } from './request.js';

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
  acks: 1,
  timeout: 30000,
  transactionalId: null,
  producerId: -1n,
  producerEpoch: 0,
  compression: COMPRESSION_TYPES.None,
  topicData: [
    {
      topic: 'test-topic-390850453b1c004039ea-1417-1c32a507-edbb-481d-9d9c-e287743f4b74',
      partitions: [{ partition: 0, firstSequence: 0, messages }],
    },
  ],
};

describe('protocol/requests/produce/v6/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV6({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes a request matching a real kafkajs fixture', async () => {
    const encoder = await produceRequestV6(args).encode();
    expect(encoder.buffer).toEqual(Buffer.from(v6RequestFixture.data));
  });
});
