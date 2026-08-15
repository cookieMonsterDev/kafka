import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { metadataResponseV1 } from './response.js';

function buildResponse(): Buffer {
  return new Encoder()
    .writeInt32(1) // brokers length
    .writeInt32(1) // nodeId
    .writeString('host')
    .writeInt32(9092) // port
    .writeString(null) // rack
    .writeInt32(1) // controllerId
    .writeInt32(1) // topicMetadata length
    .writeInt16(0) // topicErrorCode
    .writeString('my-topic')
    .writeBoolean(false) // isInternal
    .writeInt32(
      0,
    ) // partitionMetadata length
  .buffer;
}

describe('protocol/requests/metadata/v1/response', () => {
  it('decodes brokers with rack, controllerId, and isInternal', async () => {
    const data = await metadataResponseV1.decode(buildResponse());

    expect(data).toEqual({
      brokers: [{ nodeId: 1, host: 'host', port: 9092, rack: null }],
      controllerId: 1,
      topicMetadata: [{ topicErrorCode: 0, topic: 'my-topic', isInternal: false, partitionMetadata: [] }],
    });
    await expect(metadataResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
