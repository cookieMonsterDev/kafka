import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeQuorumRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-quorum/v0/request', () => {
  it('round-trips an empty topics request for metadata quorum', async () => {
    const value = { topics: [] };

    const encoder = await describeQuorumRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('round-trips a topic with explicit partitions', async () => {
    const value = {
      topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
    };

    const encoder = await describeQuorumRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
