import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { uuid, type ResponseDefinition } from '../../../schema';
import type { ShareLeaderIdAndEpoch, ShareNodeEndpoint } from '../shared';

export interface ShareAcknowledgePartitionResponseV1 {
  partitionIndex: number;
  errorCode: number;
  errorMessage: string | null;
  currentLeader: ShareLeaderIdAndEpoch;
}

export interface ShareAcknowledgeTopicResponseV1 {
  topicId: Buffer;
  partitions: ShareAcknowledgePartitionResponseV1[];
}

export interface ShareAcknowledgeResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  responses: ShareAcknowledgeTopicResponseV1[];
  nodeEndpoints: ShareNodeEndpoint[];
}

async function readCompactArrayAsync<T>(decoder: Decoder, reader: (d: Decoder) => T): Promise<T[]> {
  const encodedLength = decoder.readUVarInt();
  if (encodedLength === 0) return [];
  const length = encodedLength - 1;
  const values = new Array<T>(length);
  for (let i = 0; i < length; i++) values[i] = reader(decoder);
  return values;
}

function decodePartition(decoder: Decoder): ShareAcknowledgePartitionResponseV1 {
  const partitionIndex = decoder.readInt32();
  const errorCode = decoder.readInt16();
  const errorMessage = decoder.readUVarIntString();
  const currentLeader = {
    leaderId: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
  };
  decoder.readTaggedFields();
  return { partitionIndex, errorCode, errorMessage, currentLeader };
}

function decodeTopicResponse(decoder: Decoder): ShareAcknowledgeTopicResponseV1 {
  const topicId = uuid.read(decoder);
  const partitions = decoder.readUVarIntArray((d) => decodePartition(d)) ?? [];
  decoder.readTaggedFields();
  return { topicId, partitions };
}

function decodeNodeEndpoint(decoder: Decoder): ShareNodeEndpoint {
  const nodeId = decoder.readInt32();
  const host = decoder.readUVarIntString();
  if (host === null) throw new RangeError('Expected a non-null host, got null');
  const port = decoder.readInt32();
  const rack = decoder.readUVarIntString();
  decoder.readTaggedFields();
  return { nodeId, host, port, rack };
}

/**
 * ShareAcknowledge Response (Version: 1) => throttle_time_ms error_code error_message [responses]
 *                                         [node_endpoints] TAG_BUFFER
 *
 * Flexible from v0. AcquisitionLockTimeoutMs is v2+ and omitted. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const shareAcknowledgeResponseV1: ResponseDefinition<ShareAcknowledgeResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const errorMessage = decoder.readUVarIntString();
    const responses = await readCompactArrayAsync(decoder, decodeTopicResponse);
    const nodeEndpoints = decoder.readUVarIntArray(decodeNodeEndpoint) ?? [];
    decoder.readTaggedFields();
    return {
      throttleTime: 0,
      clientSideThrottleTime,
      errorCode,
      errorMessage,
      responses,
      nodeEndpoints,
    };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const failed = data.responses.flatMap(({ partitions }) => partitions).find(({ errorCode }) => failure(errorCode));
    if (failed) throw createErrorFromCode(failed.errorCode);
    return data;
  },
};
