import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeProducersRequestV0Topic {
  topic: string;
  partitions: number[];
}

export interface DescribeProducersRequestV0Options {
  topics: DescribeProducersRequestV0Topic[];
}

/**
 * DescribeProducers Request (Version: 0) => [topics] TAG_BUFFER
 *   topics => name [partition_indexes] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partition_indexes => INT32
 *
 * Flexible from v0. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);

export const requestSchema = flexibleObject([field('topics', compactArray(topicSchema))]);

export const describeProducersRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeProducers,
  apiVersion: 0,
  apiName: 'DescribeProducers',
  schema: requestSchema,
});
