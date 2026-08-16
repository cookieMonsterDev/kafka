/**
 * Fetch isolation. `READ_COMMITTED` hides aborted transactional records up to the last stable offset.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/#isolation.level
 */
export const ISOLATION_LEVEL = Object.freeze({
  /** All records are visible, including aborted transactional ones. */
  READ_UNCOMMITTED: 0,
  /**
   * Non-transactional and committed transactional records are visible up to the last stable
   * offset. Aborted transactional records are filtered using the aborted-transaction list.
   */
  READ_COMMITTED: 1,
});

export type IsolationLevel = (typeof ISOLATION_LEVEL)[keyof typeof ISOLATION_LEVEL];
