import { describe, expect, it } from 'vitest';
import v4ResponseFixture from '../fixtures/v4-response.json' with { type: 'json' };
import { metadataResponseV4 } from './response.js';

describe('protocol/requests/metadata/v4/response', () => {
  it('decodes a real fixture (identical wire format to v3)', async () => {
    const data = await metadataResponseV4.decode(Buffer.from(v4ResponseFixture.data));
    expect(data.clusterId).toBe('Q0WO3u_TTAeslFDJWiiGvA');
    expect(data.topicMetadata).toHaveLength(1);
    await expect(metadataResponseV4.parse(data)).resolves.toBeTruthy();
  });
});
