import { Decoder } from '../../../decoder';
import { compactNullableString, compactString, field, flexibleObject, int16, int32, object } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { findCoordinatorResponseV2, type FindCoordinatorResponseV2Body } from '../v2/response';

export type FindCoordinatorResponseV3Body = FindCoordinatorResponseV2Body;

/**
 * FindCoordinator Response (Version: 3) => throttle_time_ms error_code error_message node_id host port TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   node_id => INT32
 *   host => COMPACT_STRING
 *   port => INT32
 *
 * First flexible version (KIP-482). Coordinator fields stay nested as `coordinator` to match
 * this client. Quota timing follows v2 (KIP-219). Response header v1's trailing TAG_BUFFER is
 * skipped by `Connection` before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const coordinatorSchema = object([field('nodeId', int32), field('host', compactString), field('port', int32)]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('coordinator', coordinatorSchema),
]);

export const findCoordinatorResponseV3: ResponseDefinition<FindCoordinatorResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await findCoordinatorResponseV2.parse(data);
    return data;
  },
};
