import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { produceRequestV11 } from '../v11/request';
import { produceRequestV12 } from './request';
import type { ProduceRequestOptions } from '../shared';

const args: ProduceRequestOptions = {
  acks: -1,
  timeout: 30000,
  transactionalId: null,
  producerId: -1n,
  producerEpoch: 0,
  compression: COMPRESSION_TYPES.None,
  topicData: [
    {
      topic: 'test-topic',
      partitions: [
        {
          partition: 0,
          firstSequence: 0,
          messages: [{ key: 'k', value: 'v', timestamp: 1509928155660 }],
        },
      ],
    },
  ],
};

describe('protocol/requests/produce/v12/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV12({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes the same request body as v11', async () => {
    const v11 = await produceRequestV11(args).encode();
    const v12 = await produceRequestV12(args).encode();
    expect(produceRequestV12(args).apiVersion).toBe(12);
    expect(v12.buffer).toEqual(v11.buffer);
  });
});
