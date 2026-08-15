import { Decoder } from '../../../decoder.js';
import type { ResponseDefinition } from '../../../schema.js';
import type { FetchTopicResponseV7 } from '../v7/response.js';
import { decodeRecordSet, parseFetchResponse, readTopicName } from '../shared.js';
import type { DecodedRecordBatch } from '../../../records/batch.js';

export interface FetchPartitionResponseV8 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  lastStableOffset: bigint;
  logStartOffset: bigint;
  abortedTransactions: { producerId: bigint; firstOffset: bigint }[];
  messages: DecodedRecordBatch['records'];
}

export interface FetchResponseV8Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  sessionId: number;
  responses: FetchTopicResponseV7[];
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV8> {
  return {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64(),
    lastStableOffset: decoder.readInt64(),
    logStartOffset: decoder.readInt64(),
    abortedTransactions: decoder.readArray(() => ({
      producerId: decoder.readInt64(),
      firstOffset: decoder.readInt64(),
    })),
    messages: await decodeRecordSet(decoder),
  };
}

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV7> {
  return { topicName: readTopicName(decoder), partitions: await decoder.readArrayAsync(decodePartition) };
}

/**
 * The version bump signals that on quota violation the broker sends the response before
 * throttling (KIP-219): report a client-facing `throttleTime` of 0 and surface the wire value as
 * `clientSideThrottleTime` for the caller to act on, same as every other family's v6+-style bump.
 *
 * Fetch Response (Version: 8) - wire shape identical to v7.
 */
export const fetchResponseV8: ResponseDefinition<FetchResponseV8Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const sessionId = decoder.readInt32();
    const responses = await decoder.readArrayAsync(decodeTopicResponse);
    return { throttleTime: 0, clientSideThrottleTime, errorCode, sessionId, responses };
  },
  parse: parseFetchResponse,
};
