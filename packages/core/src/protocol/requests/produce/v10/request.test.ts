import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { produceRequestV9 } from '../v9/request';
import { produceRequestV10 } from './request';
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

describe('protocol/requests/produce/v10/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV10({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
    expect(produceRequestV10({ ...args, acks: 1 }).expectResponse?.()).toBe(true);
  });

  it('uses apiVersion 10 with the same compact body as v9', async () => {
    const definition = produceRequestV10(args);
    expect(definition.apiVersion).toBe(10);

    const [v10, v9] = await Promise.all([definition.encode(), produceRequestV9(args).encode()]);
    expect(v10.buffer).toEqual(v9.buffer);
  });
});
