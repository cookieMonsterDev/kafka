/**
 * ListOffsets Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 *
 * Wire format is identical to v2/v3. KIP-219 client-side throttle semantics (from v3) still apply.
 */
export { listOffsetsResponseV3 as listOffsetsResponseV4 } from '../v3/response';
export type { ListOffsetsResponseV3Body as ListOffsetsResponseV4Body } from '../v3/response';
