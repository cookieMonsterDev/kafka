import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { fetchResponseV2 } from './response';

describe('protocol/requests/fetch/v2/response', () => {
  it('decodes magic 1 MessageSet records with bigint timestamps', async () => {
    const data = await fetchResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.responses[0]?.topicName).toBe('test-topic-131c279f35eeb2df6bc7');
    expect(data.responses[0]?.partitions[0]?.messages).toEqual([
      expect.objectContaining({
        offset: 0n,
        magicByte: 1,
        timestamp: 1509827715172n,
        key: Buffer.from('key-0'),
        value: Buffer.from('some-value-0'),
        headers: {},
      }),
      expect.objectContaining({ offset: 1n, magicByte: 1, timestamp: 1509827715173n }),
      expect.objectContaining({ offset: 2n, magicByte: 1 }),
    ]);
    await expect(fetchResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
