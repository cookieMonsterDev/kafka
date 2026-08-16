import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { listOffsetsRequestV4 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 0,
  topics: [
    {
      topic: 'test-topic-727705ce68c29fedddf4',
      partitions: [{ partition: 0, currentLeaderEpoch: -1, timestamp: 1509285569484n }],
    },
  ],
};

describe('protocol/requests/list-offsets/v4/request', () => {
  it('round-trips replicaId, isolationLevel, currentLeaderEpoch and timestamp', async () => {
    const definition = listOffsetsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readInt8()).toBe(0);
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readString()).toBe('test-topic-727705ce68c29fedddf4');
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readInt64()).toBe(1509285569484n);
    expect(decoder.offset).toBe(encoder.buffer.length);
  });

  it('inserts currentLeaderEpoch between partition and timestamp relative to v2', async () => {
    const encoder = await listOffsetsRequestV4(payload).encode();
    const v2 = Buffer.from(v2RequestFixture.data);
    const expected = new Encoder()
      .writeBuffer(v2.subarray(0, v2.length - 8))
      .writeInt32(-1)
      .writeBuffer(v2.subarray(v2.length - 8));
    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
