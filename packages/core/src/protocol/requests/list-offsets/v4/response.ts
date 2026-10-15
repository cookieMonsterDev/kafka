/**
 * ListOffsets Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset leader_epoch
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 *       leader_epoch => INT32
 *
 * KIP-320 adds `leader_epoch` in v4. Wire format is identical to v5, which only adds error codes.
 * KIP-219 client-side throttle semantics (from v3) still apply.
 */
export { listOffsetsResponseV5 as listOffsetsResponseV4 } from '../v5/response';
export type { ListOffsetsResponseV5Body as ListOffsetsResponseV4Body } from '../v5/response';
