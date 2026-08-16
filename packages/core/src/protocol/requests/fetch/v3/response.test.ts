import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { fetchResponseV3 } from './response';

describe('protocol/requests/fetch/v3/response', () => {
  it('decodes the same MessageSet body as v1/v2', async () => {
    const data = await fetchResponseV3.decode(Buffer.from(v3ResponseFixture.data));
    expect(data.responses[0]?.topicName).toBe('test-topic-131c279f35eeb2df6bc7');
    expect(data.responses[0]?.partitions[0]?.messages.map((m) => m.magicByte)).toEqual([1, 1, 1]);
    expect(data.responses[0]?.partitions[0]?.messages[0]?.offset).toBe(0n);
    await expect(fetchResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
