import { describe, expect, it, vi } from 'vitest';
import { KafkaJSNonRetriableError } from '../../errors.js';
import { createLogger, LOG_LEVELS } from '../../loggers/index.js';
import { TransactionStateMachine } from './transaction-state-machine.js';
import { TRANSACTION_STATES } from './transaction-states.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('producer/eosManager/TransactionStateMachine', () => {
  it('starts uninitialized and reports its current state', () => {
    const machine = new TransactionStateMachine({ logger: silentLogger });
    expect(machine.state()).toBe(TRANSACTION_STATES.UNINITIALIZED);
  });

  it('allows every legal transition and rejects illegal ones', () => {
    const machine = new TransactionStateMachine({ logger: silentLogger });

    machine.transitionTo(TRANSACTION_STATES.READY);
    expect(machine.state()).toBe(TRANSACTION_STATES.READY);

    machine.transitionTo(TRANSACTION_STATES.TRANSACTING);
    expect(machine.state()).toBe(TRANSACTION_STATES.TRANSACTING);

    expect(() => machine.transitionTo(TRANSACTION_STATES.UNINITIALIZED)).toThrow(
      new KafkaJSNonRetriableError('Transaction state exception: Invalid transition TRANSACTING --> UNINITIALIZED'),
    );

    machine.transitionTo(TRANSACTION_STATES.COMMITTING);
    machine.transitionTo(TRANSACTION_STATES.READY);
  });

  it('emits a transition event with from/to on every successful transition', () => {
    const machine = new TransactionStateMachine({ logger: silentLogger });
    const listener = vi.fn();
    machine.on('transition', listener);

    machine.transitionTo(TRANSACTION_STATES.READY);

    expect(listener).toHaveBeenCalledWith({ from: TRANSACTION_STATES.UNINITIALIZED, to: TRANSACTION_STATES.READY });
  });

  describe('createGuarded', () => {
    it('runs the method when the state is legal', () => {
      const machine = new TransactionStateMachine({ logger: silentLogger, initialState: TRANSACTION_STATES.READY });
      const guarded = machine.createGuarded(
        { greet: () => 'hello' },
        { greet: { legalStates: [TRANSACTION_STATES.READY] } },
      );

      expect(guarded.greet()).toBe('hello');
    });

    it('rejects an async-guarded method called in an illegal state', async () => {
      const machine = new TransactionStateMachine({ logger: silentLogger });
      const guarded = machine.createGuarded(
        { commit: async () => 'committed' },
        { commit: { legalStates: [TRANSACTION_STATES.TRANSACTING] } },
      );

      await expect(guarded.commit()).rejects.toEqual(
        new KafkaJSNonRetriableError('Transaction state exception: Cannot call "commit" in state "UNINITIALIZED"'),
      );
    });

    it('throws synchronously for a method guarded with async: false', () => {
      const machine = new TransactionStateMachine({ logger: silentLogger });
      const guarded = machine.createGuarded(
        { beginTransaction: () => undefined },
        { beginTransaction: { legalStates: [TRANSACTION_STATES.READY], async: false } },
      );

      expect(() => guarded.beginTransaction()).toThrow(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "beginTransaction" in state "UNINITIALIZED"',
        ),
      );
    });

    it('re-checks the state at call time, not at guard-creation time', async () => {
      const machine = new TransactionStateMachine({ logger: silentLogger });
      const guarded = machine.createGuarded(
        { commit: async () => 'committed' },
        { commit: { legalStates: [TRANSACTION_STATES.TRANSACTING] } },
      );

      await expect(guarded.commit()).rejects.toThrow();

      machine.transitionTo(TRANSACTION_STATES.READY);
      machine.transitionTo(TRANSACTION_STATES.TRANSACTING);

      await expect(guarded.commit()).resolves.toBe('committed');
    });

    it('leaves methods without a declared guard untouched', () => {
      const machine = new TransactionStateMachine({ logger: silentLogger });
      const guarded = machine.createGuarded({ isTransactional: () => true }, {});
      expect(guarded.isTransactional()).toBe(true);
    });
  });
});
