import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { withAssignmentDefaults } from '../v0/request';
import { createPartitionsRequestV2, requestSchema } from '../v2/request';
import { createPartitionsRequestV3 } from './request';

const payload = {
  topicPartitions: withAssignmentDefaults([{ topic: 'orders', count: 3 }]),
  timeout: 5000,
  validateOnly: false,
};

describe('protocol/requests/create-partitions/v3/request', () => {
  it('round-trips the same wire as v2 with apiVersion 3', async () => {
    const definition = createPartitionsRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const v2 = await createPartitionsRequestV2(payload).encode();
    expect(encoder.buffer).toEqual(v2.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
