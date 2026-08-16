import { describe, expect, it } from 'vitest';
import { KafkaAggregateError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { createTopicsResponseV5 } from './response';

function encodeV5Response(options: {
  throttleTime: number;
  topics: {
    topic: string;
    errorCode: number;
    errorMessage: string | null;
    numPartitions: number;
    replicationFactor: number;
    configs:
      { name: string; value: string | null; readOnly: boolean; configSource: number; isSensitive: boolean }[] | null;
  }[];
}): Buffer {
  const encoder = new Encoder().writeInt32(options.throttleTime).writeUVarInt(options.topics.length + 1);
  for (const topic of options.topics) {
    encoder
      .writeUVarIntString(topic.topic)
      .writeInt16(topic.errorCode)
      .writeUVarIntString(topic.errorMessage)
      .writeInt32(topic.numPartitions)
      .writeInt16(topic.replicationFactor);
    if (topic.configs === null) {
      encoder.writeUVarInt(0);
    } else {
      encoder.writeUVarInt(topic.configs.length + 1);
      for (const config of topic.configs) {
        encoder
          .writeUVarIntString(config.name)
          .writeUVarIntString(config.value)
          .writeBoolean(config.readOnly)
          .writeInt8(config.configSource)
          .writeBoolean(config.isSensitive)
          .writeUVarInt(0);
      }
    }
    encoder.writeUVarInt(0);
  }
  return encoder.writeUVarInt(0).buffer;
}

describe('protocol/requests/create-topics/v5/response', () => {
  it('decodes a flexible body with configs and remaps throttleTime', async () => {
    const data = await createTopicsResponseV5.decode(
      encodeV5Response({
        throttleTime: 8,
        topics: [
          {
            topic: 'payments',
            errorCode: 0,
            errorMessage: null,
            numPartitions: 3,
            replicationFactor: 2,
            configs: null,
          },
          {
            topic: 'orders',
            errorCode: 0,
            errorMessage: null,
            numPartitions: 3,
            replicationFactor: 2,
            configs: [
              { name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: 5, isSensitive: false },
            ],
          },
        ],
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topicErrors: [
        {
          topic: 'orders',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: [{ name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: 5, isSensitive: false }],
        },
        {
          topic: 'payments',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: null,
        },
      ],
    });
    await expect(createTopicsResponseV5.parse(data)).resolves.toBe(data);
  });

  it('throws KafkaAggregateError when a topic has an error', async () => {
    const data = await createTopicsResponseV5.decode(
      encodeV5Response({
        throttleTime: 0,
        topics: [
          {
            topic: 'orders',
            errorCode: 36,
            errorMessage: 'already exists',
            numPartitions: -1,
            replicationFactor: -1,
            configs: null,
          },
        ],
      }),
    );

    await expect(createTopicsResponseV5.parse(data)).rejects.toThrow(KafkaAggregateError);
  });
});
