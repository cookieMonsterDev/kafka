import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { DecodedRecordBatch } from '../../../records/batch';
import { uuid, type ResponseDefinition } from '../../../schema';
import { decodeCompactRecordSet } from '../../fetch/shared';
import type { ShareAcquiredRecords, ShareLeaderIdAndEpoch, ShareNodeEndpoint } from '../shared';

export interface ShareFetchPartitionResponseV1 {
  partitionIndex: number;
  errorCode: number;
  errorMessage: string | null;
  acknowledgeErrorCode: number;
  acknowledgeErrorMessage: string | null;
  currentLeader: ShareLeaderIdAndEpoch;
  records: DecodedRecordBatch['records'];
  acquiredRecords: ShareAcquiredRecords[];
}

export interface ShareFetchTopicResponseV1 {
  topicId: Buffer;
  partitions: ShareFetchPartitionResponseV1[];
}

export interface ShareFetchResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  acquisitionLockTimeoutMs: number;
  responses: ShareFetchTopicResponseV1[];
  nodeEndpoints: ShareNodeEndpoint[];
}

async function readCompactArrayAsync<T>(decoder: Decoder, reader: (d: Decoder) => Promise<T>): Promise<T[]> {
  const encodedLength = decoder.readUVarInt();
  if (encodedLength === 0) return [];
  const length = encodedLength - 1;
  const values = new Array<T>(length);
  for (let i = 0; i < length; i++) values[i] = await reader(decoder);
  return values;
}

async function decodeAcquiredRecords(decoder: Decoder): Promise<ShareAcquiredRecords> {
  const firstOffset = decoder.readInt64();
  const lastOffset = decoder.readInt64();
  const deliveryCount = decoder.readInt16();
  decoder.readTaggedFields();
  return { firstOffset, lastOffset, deliveryCount };
}

async function decodePartition(decoder: Decoder): Promise<ShareFetchPartitionResponseV1> {
  const partitionIndex = decoder.readInt32();
  const errorCode = decoder.readInt16();
  const errorMessage = decoder.readUVarIntString();
  const acknowledgeErrorCode = decoder.readInt16();
  const acknowledgeErrorMessage = decoder.readUVarIntString();
  const currentLeader = {
    leaderId: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
  };
  decoder.readTaggedFields();
  const records = await decodeCompactRecordSet(decoder);
  const acquiredRecords = await readCompactArrayAsync(decoder, decodeAcquiredRecords);
  decoder.readTaggedFields();
  return {
    partitionIndex,
    errorCode,
    errorMessage,
    acknowledgeErrorCode,
    acknowledgeErrorMessage,
    currentLeader,
    records,
    acquiredRecords,
  };
}

async function decodeTopicResponse(decoder: Decoder): Promise<ShareFetchTopicResponseV1> {
  const topicId = uuid.read(decoder);
  const partitions = await readCompactArrayAsync(decoder, decodePartition);
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
 * ShareFetch Response (Version: 1) => throttle_time_ms error_code error_message
 *                                    acquisition_lock_timeout_ms [responses] [node_endpoints] TAG_BUFFER
 *   responses => topic_id [partitions] TAG_BUFFER
 *     partitions => partition_index error_code error_message acknowledge_error_code
 *                   acknowledge_error_message current_leader records [acquired_records] TAG_BUFFER
 *       records => COMPACT_RECORDS
 *       acquired_records => first_offset last_offset delivery_count TAG_BUFFER
 *
 * Flexible from v0. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const shareFetchResponseV1: ResponseDefinition<ShareFetchResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const errorMessage = decoder.readUVarIntString();
    const acquisitionLockTimeoutMs = decoder.readInt32();
    const responses = await readCompactArrayAsync(decoder, decodeTopicResponse);
    const nodeEndpoints = decoder.readUVarIntArray(decodeNodeEndpoint) ?? [];
    decoder.readTaggedFields();
    return {
      throttleTime: 0,
      clientSideThrottleTime,
      errorCode,
      errorMessage,
      acquisitionLockTimeoutMs,
      responses,
      nodeEndpoints,
    };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const failed = data.responses
      .flatMap(({ partitions }) => partitions)
      .find(({ errorCode, acknowledgeErrorCode }) => failure(errorCode) || failure(acknowledgeErrorCode));
    if (failed) {
      throw createErrorFromCode(failure(failed.errorCode) ? failed.errorCode : failed.acknowledgeErrorCode);
    }
    return data;
  },
};
