import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { txnOffsetCommitResponseV2, type TxnOffsetCommitResponseV2Body } from '../v2/response';

export type TxnOffsetCommitResponseV3Body = TxnOffsetCommitResponseV2Body;

/**
 * TxnOffsetCommit Response (Version: 3) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *
 * First flexible version (KIP-482). Same response fields as v0–v2. Quota timing follows v1
 * (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partition', int32), field('errorCode', int16)]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topics', compactArray(topicSchema))]);

export const txnOffsetCommitResponseV3: ResponseDefinition<TxnOffsetCommitResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await txnOffsetCommitResponseV2.parse(data);
    return data;
  },
};
