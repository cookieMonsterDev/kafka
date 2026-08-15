import { describe, expect, it } from 'vitest'
import v5OfflineReplicasResponseFixture from '../fixtures/v5-offline-replicas-response.json' with { type: 'json' }
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' }
import { metadataResponseV5 } from './response.js'

describe('protocol/requests/metadata/v5/response', () => {
  it('decodes a real fixture, including empty offlineReplicas', async () => {
    const data = await metadataResponseV5.decode(Buffer.from(v5ResponseFixture.data))

    expect(data).toEqual({
      throttleTime: 0,
      brokers: [
        { nodeId: 2, host: '10.3.220.89', port: 9098, rack: null },
        { nodeId: 1, host: '10.3.220.89', port: 9095, rack: null },
        { nodeId: 0, host: '10.3.220.89', port: 9092, rack: null },
      ],
      clusterId: 'wyOEk0m7Tn-08oGZjtVgEg',
      controllerId: 2,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-f5e17a86896ebfdeb429-80829-a37b6dde-1adc-4687-813d-52d75a0a0f78',
          isInternal: false,
          partitionMetadata: [
            { partitionErrorCode: 0, partitionId: 0, leader: 0, replicas: [0], isr: [0], offlineReplicas: [] },
          ],
        },
      ],
    })
    await expect(metadataResponseV5.parse(data)).resolves.toBeTruthy()
  })

  it('throws on a real fixture with a genuine offline-replicas election error', async () => {
    const data = await metadataResponseV5.decode(Buffer.from(v5OfflineReplicasResponseFixture.data))
    expect(data.topicMetadata[0]?.partitionMetadata[0]).toEqual({
      isr: [],
      leader: -1,
      offlineReplicas: [2],
      partitionErrorCode: 5,
      partitionId: 2,
      replicas: [2],
    })
    await expect(metadataResponseV5.parse(data)).rejects.toThrow(
      'There is no leader for this topic-partition as we are in the middle of a leadership election'
    )
  })
})
