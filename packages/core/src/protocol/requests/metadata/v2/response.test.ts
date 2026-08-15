import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { metadataResponseV2 } from './response.js';

describe('protocol/requests/metadata/v2/response', () => {
  it('decodes clusterId alongside the v1 fields', async () => {
    const wire = new Encoder()
      .writeInt32(0) // brokers length
      .writeString('cluster-id')
      .writeInt32(1) // controllerId
      .writeInt32(
        0,
      ) // topicMetadata length
    .buffer;

    const data = await metadataResponseV2.decode(wire);
    expect(data).toEqual({ brokers: [], clusterId: 'cluster-id', controllerId: 1, topicMetadata: [] });
    await expect(metadataResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
