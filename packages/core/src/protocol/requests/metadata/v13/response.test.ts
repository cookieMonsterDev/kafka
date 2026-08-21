import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED } from '../shared';
import { metadataResponseV13 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function writeCompactInt32Array(encoder: Encoder, values: number[]): Encoder {
  encoder.writeUVarInt(values.length + 1);
  for (const value of values) encoder.writeInt32(value);
  return encoder;
}

function encodeV13Response({
  throttleTime = 0,
  topicErrorCode = 0,
  errorCode = 0,
  topicName = 'orders',
}: {
  throttleTime?: number;
  topicErrorCode?: number;
  errorCode?: number;
  topicName?: string | null;
} = {}): Buffer {
  const partition = new Encoder().writeInt16(0).writeInt32(0).writeInt32(1).writeInt32(5);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, []);
  partition.writeUVarInt(0);

  const topic = new Encoder()
    .writeInt16(topicErrorCode)
    .writeUVarIntString(topicName)
    .writeBuffer(topicId)
    .writeBoolean(false)
    .writeUVarInt(2)
    .writeEncoder(partition)
    .writeInt32(AUTHORIZED_OPERATIONS_OMITTED)
    .writeUVarInt(0);

  const broker = new Encoder()
    .writeInt32(1)
    .writeUVarIntString('localhost')
    .writeInt32(9092)
    .writeUVarIntString(null)
    .writeUVarInt(0);

  return new Encoder()
    .writeInt32(throttleTime)
    .writeUVarInt(2)
    .writeEncoder(broker)
    .writeUVarIntString('cluster-1')
    .writeInt32(1)
    .writeUVarInt(2)
    .writeEncoder(topic)
    .writeInt16(errorCode)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/metadata/v13/response', () => {
  it('decodes the top-level errorCode and remaps throttleTime', async () => {
    const data = await metadataResponseV13.decode(encodeV13Response({ throttleTime: 4 }));

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(4);
    expect(data.errorCode).toBe(0);
    expect(data.topicMetadata[0]?.topic).toBe('orders');
    expect(data.topicMetadata[0]?.topicId).toEqual(topicId);
    expect(data).not.toHaveProperty('clusterAuthorizedOperations');
    await expect(metadataResponseV13.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a top-level error', async () => {
    const data = await metadataResponseV13.decode(encodeV13Response({ errorCode: 13 }));
    await expect(metadataResponseV13.parse(data)).rejects.toMatchObject({
      type: 'NETWORK_EXCEPTION',
    });
  });

  it('decodes a nullable topic name', async () => {
    const data = await metadataResponseV13.decode(encodeV13Response({ topicErrorCode: 3, topicName: null }));
    expect(data.topicMetadata[0]?.topic).toBeNull();
    await expect(metadataResponseV13.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
  });
});
