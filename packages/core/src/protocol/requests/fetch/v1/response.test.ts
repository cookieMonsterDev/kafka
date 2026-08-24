import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import v1ResponseGzipFixture from '../fixtures/v1-response-gzip.json' with { type: 'json' };
import { fetchResponseV1 } from './response';

describe('protocol/requests/fetch/v1/response', () => {
  it('decodes throttleTime plus a MessageSet partition', async () => {
    const data = await fetchResponseV1().decode(Buffer.from(v1ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.responses[0]?.topicName).toBe('test-topic-6354595aa07c0fa2ae55');
    expect(data.responses[0]?.partitions[0]?.highWatermark).toBe(1n);
    expect(data.responses[0]?.partitions[0]?.messages[0]).toEqual(
      expect.objectContaining({
        offset: 0n,
        magicByte: 0,
        key: Buffer.from('key-0'),
        value: Buffer.from('some-value-0'),
        headers: {},
      }),
    );
    await expect(fetchResponseV1().parse(data)).resolves.toBeTruthy();
  });

  it('decodes a gzip MessageSet fixture', async () => {
    const data = await fetchResponseV1().decode(Buffer.from(v1ResponseGzipFixture.data));
    expect(data.responses[0]?.partitions[0]?.messages.map((m) => m.offset)).toEqual([0n, 1n, 2n]);
    expect(data.responses[0]?.topicName).toBe('test-topic-ae0b74cd45cb7c1971dd');
  });
});
