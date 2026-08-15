export const ISOLATION_LEVEL = Object.freeze({
  // Makes all records visible.
  READ_UNCOMMITTED: 0,
  // Non-transactional and COMMITTED transactional records are visible, up to the current LSO
  // (last stable offset); includes the list of aborted transactions so consumers can discard
  // ABORTED transactional records.
  READ_COMMITTED: 1,
})

export type IsolationLevel = (typeof ISOLATION_LEVEL)[keyof typeof ISOLATION_LEVEL]
