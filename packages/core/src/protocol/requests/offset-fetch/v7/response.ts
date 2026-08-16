/**
 * OffsetFetch Response (Version: 7) — same wire as v6 (flexible v5, including leader_epoch).
 * Partition-level UNSTABLE_OFFSET_COMMIT is a new error code on this version, not a new field.
 */
export { offsetFetchResponseV6 as offsetFetchResponseV7 } from '../v6/response';
export type { OffsetFetchResponseV6Body as OffsetFetchResponseV7Body } from '../v6/response';
