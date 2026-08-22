import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeQuorumRequestV2, requestSchema } from './request';

describe('protocol/requests/describe-quorum/v2/request', () => {
  it('round-trips the same body as v0', async () => {
    const value = {
      topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
    };
    const encoder = await describeQuorumRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
