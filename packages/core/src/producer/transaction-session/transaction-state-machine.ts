import { EventEmitter } from 'node:events';
import { KafkaNonRetriableError } from '../../errors';
import type { Logger } from '../../loggers/index';
import { TRANSACTION_STATES, type TransactionState } from './transaction-states';

/**
 * Typed as a `Record<TransactionState, ...>` (rather than a bare object literal) so this is
 * exhaustive at compile time: dropping a state here, or typo-ing one of the transition targets, is
 * a type error instead of a transition silently missing at runtime. The annotation adds no runtime
 * code of its own - `erasableSyntaxOnly` stays satisfied - the checking is pure type-level.
 */
const VALID_STATE_TRANSITIONS: Record<TransactionState, readonly TransactionState[]> = {
  [TRANSACTION_STATES.UNINITIALIZED]: [TRANSACTION_STATES.READY],
  [TRANSACTION_STATES.READY]: [TRANSACTION_STATES.READY, TRANSACTION_STATES.TRANSACTING],
  [TRANSACTION_STATES.TRANSACTING]: [
    TRANSACTION_STATES.COMMITTING,
    TRANSACTION_STATES.ABORTING,
    TRANSACTION_STATES.ABORTABLE,
  ],
  [TRANSACTION_STATES.ABORTABLE]: [TRANSACTION_STATES.ABORTABLE, TRANSACTION_STATES.ABORTING],
  [TRANSACTION_STATES.COMMITTING]: [
    TRANSACTION_STATES.READY,
    TRANSACTION_STATES.ABORTABLE,
    TRANSACTION_STATES.FATAL_ERROR,
  ],
  [TRANSACTION_STATES.ABORTING]: [TRANSACTION_STATES.READY, TRANSACTION_STATES.FATAL_ERROR],
  [TRANSACTION_STATES.FATAL_ERROR]: [],
};

/** Every call and transition out of `FATAL_ERROR` rejects with this, instead of a generic state error. */
export const FENCED_PRODUCER_MESSAGE =
  'The producer was fenced by a newer instance with the same transactionalId; create a new producer';

export interface TransitionEvent {
  from: TransactionState;
  to: TransactionState;
}

/** The transaction states and the legal transitions between them. */
export class TransactionStateMachine extends EventEmitter {
  readonly #logger: Logger;
  #currentState: TransactionState;

  constructor({
    logger,
    initialState = TRANSACTION_STATES.UNINITIALIZED,
  }: {
    logger: Logger;
    initialState?: TransactionState;
  }) {
    super();
    this.#logger = logger;
    this.#currentState = initialState;
  }

  state(): TransactionState {
    return this.#currentState;
  }

  transitionTo(state: TransactionState): void {
    this.#logger.debug(`Transaction state transition ${this.#currentState} --> ${state}`);

    if (!VALID_STATE_TRANSITIONS[this.#currentState].includes(state)) {
      throw this.#stateError(`Invalid transition ${this.#currentState} --> ${state}`);
    }

    const from = this.#currentState;
    this.#currentState = state;
    this.emit('transition', { to: state, from } satisfies TransitionEvent);
  }

  /** The error for `call` made in the current state: the fenced error from `FATAL_ERROR`, a state error otherwise. */
  illegalCallError(call: string): KafkaNonRetriableError {
    return this.#stateError(`Cannot call "${call}" in state "${this.#currentState}"`);
  }

  #stateError(detail: string): KafkaNonRetriableError {
    if (this.#currentState === TRANSACTION_STATES.FATAL_ERROR) {
      return new KafkaNonRetriableError(FENCED_PRODUCER_MESSAGE);
    }
    return new KafkaNonRetriableError(`Transaction state exception: ${detail}`);
  }
}
