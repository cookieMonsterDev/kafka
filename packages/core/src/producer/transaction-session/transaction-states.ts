export const TRANSACTION_STATES = Object.freeze({
  UNINITIALIZED: 'UNINITIALIZED',
  READY: 'READY',
  TRANSACTING: 'TRANSACTING',
  /** KIP-890 TRANSACTION_ABORTABLE: the broker rejected a produce, only `abort()` is legal from here. */
  ABORTABLE: 'ABORTABLE',
  COMMITTING: 'COMMITTING',
  ABORTING: 'ABORTING',
  /** A newer producer with this transactionalId fenced this one: every further call rejects. */
  FATAL_ERROR: 'FATAL_ERROR',
});

export type TransactionState = (typeof TRANSACTION_STATES)[keyof typeof TRANSACTION_STATES];
