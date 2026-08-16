import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { addPartitionsToTxnResponseV2, type AddPartitionsToTxnResponseV2Body } from '../v2/response';

export type AddPartitionsToTxnResponseV3Body = AddPartitionsToTxnResponseV2Body;

/**
 * AddPartitionsToTxn Response (Version: 3) => throttle_time_ms [results_by_topic] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results_by_topic => name [results_by_partition] TAG_BUFFER
 *     name => COMPACT_STRING
 *     results_by_partition => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *
 * First flexible version (KIP-482). Same fields as v0–v2. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionErrorSchema = flexibleObject([field('partition', int32), field('errorCode', int16)]);
const topicErrorsSchema = flexibleObject([
  field('topic', compactString),
  field('partitionErrors', compactArray(partitionErrorSchema)),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('errors', compactArray(topicErrorsSchema))]);

export const addPartitionsToTxnResponseV3: ResponseDefinition<AddPartitionsToTxnResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await addPartitionsToTxnResponseV2.parse(data);
    return data;
  },
};
