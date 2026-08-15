import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { metadataResponseV3 } from './response.js';

describe('protocol/requests/metadata/v3/response', () => {
  it('decodes a real fixture', async () => {
    const data = await metadataResponseV3.decode(Buffer.from(v3ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      brokers: [
        { nodeId: 2, host: '192.168.1.173', port: 9098, rack: null },
        { nodeId: 1, host: '192.168.1.173', port: 9095, rack: null },
        { nodeId: 0, host: '192.168.1.173', port: 9092, rack: null },
      ],
      clusterId: 'Q0WO3u_TTAeslFDJWiiGvA',
      controllerId: 1,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-0f67c79007c9157fc83d',
          isInternal: false,
          partitionMetadata: [{ partitionErrorCode: 0, partitionId: 0, leader: 2, replicas: [2], isr: [2] }],
        },
      ],
    });
    await expect(metadataResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
