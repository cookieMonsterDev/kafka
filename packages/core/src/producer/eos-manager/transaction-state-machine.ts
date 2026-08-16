import { EventEmitter } from 'node:events';
import { KafkaJSNonRetriableError } from '../../errors.js';
import type { Logger } from '../../loggers/index.js';
import { TRANSACTION_STATES, type TransactionState } from './transaction-states.js';

/**
 * Typed as a `Record<TransactionState, ...>` (rather than a bare object literal) so this is
 * exhaustive at compile time: dropping a state here, or typo-ing one of the transition targets, is
 * a type error instead of a transition silently missing at runtime. The annotation adds no runtime
 * code of its own - `erasableSyntaxOnly` stays satisfied - the checking is pure type-level.
 */
const VALID_STATE_TRANSITIONS: Record<TransactionState, readonly TransactionState[]> = {
  [TRANSACTION_STATES.UNINITIALIZED]: [TRANSACTION_STATES.READY],
  [TRANSACTION_STATES.READY]: [TRANSACTION_STATES.READY, TRANSACTION_STATES.TRANSACTING],
  [TRANSACTION_STATES.TRANSACTING]: [TRANSACTION_STATES.COMMITTING, TRANSACTION_STATES.ABORTING],
  [TRANSACTION_STATES.COMMITTING]: [TRANSACTION_STATES.READY],
  [TRANSACTION_STATES.ABORTING]: [TRANSACTION_STATES.READY],
};

export interface TransitionEvent {
  from: TransactionState;
  to: TransactionState;
}

export interface TransactionStateGuard {
  legalStates: readonly TransactionState[];
  /** Defaults to `true` (an illegal call rejects). Set `false` for a synchronous method, which throws instead. */
  async?: boolean;
}

/**
 * Guards a plain object's methods so each only runs while the machine is in one of its declared
 * `legalStates` - otherwise it rejects (or, for `async: false`, throws synchronously) with a
 * `KafkaJSNonRetriableError` instead of running at all.
 */
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
      throw new KafkaJSNonRetriableError(
        `Transaction state exception: Invalid transition ${this.#currentState} --> ${state}`,
      );
    }

    const from = this.#currentState;
    this.#currentState = state;
    this.emit('transition', { to: state, from } satisfies TransitionEvent);
  }

  createGuarded<T extends object>(object: T, methodStateGuards: Partial<Record<keyof T, TransactionStateGuard>>): T {
    const rawObject = object as Record<string, unknown>;
    const guarded: Record<string, unknown> = { ...rawObject };

    for (const key of Object.keys(methodStateGuards)) {
      const guard = methodStateGuards[key as keyof T];
      if (!guard) continue;

      const fn = rawObject[key];
      if (typeof fn !== 'function') {
        throw new KafkaJSNonRetriableError(`Cannot add guard on missing method "${key}"`);
      }

      const { legalStates, async: isAsync = true } = guard;

      guarded[key] = (...args: unknown[]): unknown => {
        if (!legalStates.includes(this.#currentState)) {
          const error = new KafkaJSNonRetriableError(
            `Transaction state exception: Cannot call "${key}" in state "${this.#currentState}"`,
          );
          if (isAsync) return Promise.reject(error);
          throw error;
        }

        return (fn as (...fnArgs: unknown[]) => unknown).apply(rawObject, args);
      };
    }

    return guarded as T;
  }
}
