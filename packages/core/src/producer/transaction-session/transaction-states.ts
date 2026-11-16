export const TRANSACTION_STATES = Object.freeze({
  UNINITIALIZED: 'UNINITIALIZED',
  READY: 'READY',
  TRANSACTING: 'TRANSACTING',
  /** A send, `sendOffsets` or commit failed (like KIP-890's TRANSACTION_ABORTABLE): only `abort()` is legal from here. */
  ABORTABLE: 'ABORTABLE',
  COMMITTING: 'COMMITTING',
  ABORTING: 'ABORTING',
  /** A newer producer with this transactionalId fenced this one: every further call rejects. */
  FATAL_ERROR: 'FATAL_ERROR',
});

export type TransactionState = (typeof TRANSACTION_STATES)[keyof typeof TRANSACTION_STATES];
