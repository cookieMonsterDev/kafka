/**
 * OffsetFetch Response (Version: 9) — same wire as v8. With a member id in the request, a group
 * can also answer STALE_MEMBER_EPOCH or UNKNOWN_MEMBER_ID (KIP-848).
 */
export { offsetFetchResponseV8 as offsetFetchResponseV9 } from '../v8/response';
export type { OffsetFetchResponseV8Body as OffsetFetchResponseV9Body } from '../v8/response';
