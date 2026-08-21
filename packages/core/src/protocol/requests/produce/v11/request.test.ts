import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { produceRequestV10 } from '../v10/request';
import { produceRequestV11 } from './request';
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

describe('protocol/requests/produce/v11/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV11({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes the same request body as v10', async () => {
    const v10 = await produceRequestV10(args).encode();
    const v11 = await produceRequestV11(args).encode();
    expect(produceRequestV11(args).apiVersion).toBe(11);
    expect(v11.buffer).toEqual(v10.buffer);
  });
});
