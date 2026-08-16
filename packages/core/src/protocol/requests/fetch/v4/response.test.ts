import { describe, expect, it } from 'vitest';
import v4ResponseFixture from '../fixtures/v4-response.json' with { type: 'json' };
import v4Response010Fixture from '../fixtures/v4-response-010-format.json' with { type: 'json' };
import v4ResponseMixedFixture from '../fixtures/v4-response-mixed-formats.json' with { type: 'json' };
import { fetchResponseV4 } from './response';

const batchContext = {
  firstOffset: expect.any(BigInt),
  firstSequence: expect.any(Number),
  firstTimestamp: expect.any(BigInt),
  inTransaction: expect.any(Boolean),
  isControlBatch: expect.any(Boolean),
  lastOffsetDelta: expect.any(Number),
  magicByte: expect.any(Number),
  maxTimestamp: expect.any(BigInt),
  partitionLeaderEpoch: expect.any(Number),
  producerEpoch: expect.any(Number),
  producerId: expect.any(BigInt),
  timestampType: expect.any(Number),
};

describe('protocol/requests/fetch/v4/response', () => {
  it('decodes a real fixture, including uncompressed record batches', async () => {
    const data = await fetchResponseV4.decode(Buffer.from(v4ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-ab4d54774dcadc395a7f',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 3n,
              lastStableOffset: 3n,
              abortedTransactions: [],
              messages: [0, 1, 2].map((i) => ({
                offset: BigInt(i),
                magicByte: 2,
                attributes: 0,
                batchContext,
                timestamp: 1509827900073n,
                headers: { [`header-key-${i}`]: Buffer.from(`header-value-${i}`) },
                key: Buffer.from(`key-${i}`),
                value: Buffer.from(`some-value-${i}`),
                isControlRecord: false,
              })),
            },
          ],
        },
      ],
    });

    await expect(fetchResponseV4.parse(data)).resolves.toBeTruthy();
  });

  it('decodes a 0.10 MessageSet payload (magic 1) from a v4 response', async () => {
    const data = await fetchResponseV4.decode(Buffer.from(v4Response010Fixture.data));
    expect(data.responses[0]?.partitions[0]?.messages).toEqual([
      expect.objectContaining({
        offset: 0n,
        size: 158,
        crc: 2036710961,
        magicByte: 1,
        attributes: 0,
        timestamp: 1538502423117n,
        key: Buffer.from('key-9bf6284dc11345082649-78767-f79b4780-f2aa-4bbb-979f-9a4815652b5c'),
        value: Buffer.from('value-9bf6284dc11345082649-78767-f79b4780-f2aa-4bbb-979f-9a4815652b5c'),
        headers: {},
      }),
    ]);
    await expect(fetchResponseV4.parse(data)).resolves.toBeTruthy();
  });

  it('decodes only the 0.10 messages from a mixed-format response', async () => {
    const data = await fetchResponseV4.decode(Buffer.from(v4ResponseMixedFixture.data));
    const magicBytes = data.responses[0]?.partitions[0]?.messages.map((m) => m.magicByte) ?? [];
    expect(new Set(magicBytes)).toEqual(new Set([1]));
  });
});
