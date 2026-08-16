import {
  boolean,
  compactArray,
  compactNullableArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  taggedFields,
} from '../../../schema';
import type { FieldCodec } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * CreatePartitions Request (Version: 2) => [topics] timeout_ms validate_only TAG_BUFFER
 *   topics => name count [assignments] TAG_BUFFER
 *     name => COMPACT_STRING
 *     count => INT32
 *     assignments => [broker_ids] TAG_BUFFER
 *       broker_ids => INT32
 *   timeout_ms => INT32
 *   validate_only => BOOLEAN
 *
 * First flexible version (KIP-482). Nested `assignments` is a nullable compact array of
 * assignment structs (compact int32 arrays + TAG_BUFFER). Empty assignments encode as null to
 * match v0 `nullableArray` semantics. Request header v2's trailing TAG_BUFFER is written by
 * `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const brokerIdsSchema = compactArray(int32);
const assignmentBrokerIds: FieldCodec<number[]> = {
  write(encoder, brokerIds) {
    brokerIdsSchema.write(encoder, brokerIds);
    taggedFields.write(encoder, null);
  },
  read(decoder) {
    const brokerIds = brokerIdsSchema.read(decoder);
    taggedFields.read(decoder);
    return brokerIds;
  },
};
const nullableAssignments = compactNullableArray(assignmentBrokerIds);
const assignmentsSchema: FieldCodec<number[][]> = {
  write(encoder, assignments) {
    nullableAssignments.write(encoder, assignments.length === 0 ? null : assignments);
  },
  read(decoder) {
    return nullableAssignments.read(decoder) ?? [];
  },
};

const topicPartitionSchema = flexibleObject([
  field('topic', compactString),
  field('count', int32),
  field('assignments', assignmentsSchema),
]);
export const requestSchema = flexibleObject([
  field('topicPartitions', compactArray(topicPartitionSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export function createFlexibleCreatePartitionsRequest(apiVersion: 2 | 3) {
  return defineRequest({
    apiKey: API_KEYS.CreatePartitions,
    apiVersion,
    apiName: 'CreatePartitions',
    schema: requestSchema,
  });
}

export const createPartitionsRequestV2 = createFlexibleCreatePartitionsRequest(2);
