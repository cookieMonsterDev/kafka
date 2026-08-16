/**
 * Produce Response (Version: 7) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *   throttle_time_ms => INT32
 *
 * Wire shape and parsing identical to v6.
 */
export { produceResponseV6 as produceResponseV7 } from '../v6/response';
export type { ProduceResponseV6Body as ProduceResponseV7Body } from '../v6/response';
