import { COMPRESSION_TYPES, lookupCodec, type CompressionType } from '../../compression/index';
import { Encoder } from '../../encoder';
import { encodeMessageSet } from '../../message-set/index';
import { array, bytes, field, int16, int32, object, string, type RequestDefinition } from '../../schema';
import { API_KEYS } from '../api-keys';
import type { ProducePartitionData, ProduceRequestOptions } from './shared';

const requestBodySchema = object([
  field('acks', int16),
  field('timeout', int32),
  field(
    'topicData',
    array(
      object([
        field('topic', string),
        field('partitions', array(object([field('partition', int32), field('recordSet', bytes)]))),
      ]),
    ),
  ),
]);

async function encodePartition(
  { partition, messages }: ProducePartitionData,
  { apiVersion, compression }: { apiVersion: number; compression: CompressionType },
): Promise<{ partition: number; recordSet: Buffer }> {
  const messageVersion = apiVersion >= 2 ? 1 : 0;
  const messageSet = encodeMessageSet({
    messageVersion,
    compression: apiVersion >= 2 ? compression : COMPRESSION_TYPES.None,
    entries: messages,
  });

  if (apiVersion < 2 || compression === COMPRESSION_TYPES.None) {
    return { partition, recordSet: messageSet.buffer };
  }

  const codec = lookupCodec(compression);
  if (!codec) {
    throw new Error(`Invariant violated: no codec registered for compression type ${compression}`);
  }

  const timestamp = messages[0]?.timestamp ?? Date.now();
  const compressedValue = await codec.compress(messageSet);
  const compressedMessageSet = encodeMessageSet({
    messageVersion: 1,
    entries: [{ compression, timestamp, value: compressedValue }],
  });

  return { partition, recordSet: compressedMessageSet.buffer };
}

/**
 * Produce v0–v2 send the legacy MessageSet (magic 0/1). v0/v1 are uncompressed magic 0;
 * v2 uses magic 1 and may wrap the set in a single compressed message.
 */
export function createMessageSetProduceRequest(
  apiVersion: 0 | 1 | 2,
  options: ProduceRequestOptions,
): RequestDefinition {
  const { acks, timeout, topicData } = options;
  const compression = options.compression ?? COMPRESSION_TYPES.None;

  return {
    apiKey: API_KEYS.Produce,
    apiVersion,
    apiName: 'Produce',
    expectResponse: () => acks !== 0,
    encode: async () => {
      const encodedTopicData = await Promise.all(
        topicData.map(async ({ topic, partitions }) => ({
          topic,
          partitions: await Promise.all(
            partitions.map((partition) => encodePartition(partition, { apiVersion, compression })),
          ),
        })),
      );

      const encoder = new Encoder();
      requestBodySchema.write(encoder, { acks, timeout, topicData: encodedTopicData });
      return encoder;
    },
  };
}
