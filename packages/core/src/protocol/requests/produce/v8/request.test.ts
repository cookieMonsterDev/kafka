import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { produceRequestV7 } from '../v7/request';
import { produceRequestV8 } from './request';
import type { ProduceRequestOptions } from '../shared';

const args: ProduceRequestOptions = {
  acks: -1,
  timeout: 30000,
  transactionalId: null,
  producerId: 4004n,
  producerEpoch: 0,
  compression: COMPRESSION_TYPES.None,
  topicData: [
    {
      topic: 'test-topic',
      partitions: [{ partition: 0, firstSequence: 0, messages: [{ key: 'k', value: 'v', timestamp: 1509928155660 }] }],
    },
  ],
};

describe('protocol/requests/produce/v8/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV8({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes the same request body as v7', async () => {
    const v7 = await produceRequestV7(args).encode();
    const v8 = await produceRequestV8(args).encode();
    expect(produceRequestV8(args).apiVersion).toBe(8);
    expect(v8.buffer).toEqual(v7.buffer);
  });
});
