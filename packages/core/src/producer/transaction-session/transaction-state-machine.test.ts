import { describe, expect, it, vi } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { FENCED_PRODUCER_MESSAGE, TransactionStateMachine } from './transaction-state-machine';
import { TRANSACTION_STATES } from './transaction-states';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('producer/transactionSession/TransactionStateMachine', () => {
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
      new KafkaNonRetriableError('Transaction state exception: Invalid transition TRANSACTING --> UNINITIALIZED'),
    );

    machine.transitionTo(TRANSACTION_STATES.COMMITTING);
    machine.transitionTo(TRANSACTION_STATES.READY);
  });

  it('allows TRANSACTING to go abort-only (KIP-890 TRANSACTION_ABORTABLE) and only out via abort', () => {
    const machine = new TransactionStateMachine({ logger: silentLogger });
    machine.transitionTo(TRANSACTION_STATES.READY);
    machine.transitionTo(TRANSACTION_STATES.TRANSACTING);
    machine.transitionTo(TRANSACTION_STATES.ABORTABLE);
    expect(machine.state()).toBe(TRANSACTION_STATES.ABORTABLE);

    expect(() => machine.transitionTo(TRANSACTION_STATES.COMMITTING)).toThrow(
      new KafkaNonRetriableError('Transaction state exception: Invalid transition ABORTABLE --> COMMITTING'),
    );

    machine.transitionTo(TRANSACTION_STATES.ABORTING);
    machine.transitionTo(TRANSACTION_STATES.READY);
    expect(machine.state()).toBe(TRANSACTION_STATES.READY);
  });

  it('lets a rejected commit go abort-only and a fenced end go fatal, with no way out of FATAL_ERROR', () => {
    const machine = new TransactionStateMachine({
      logger: silentLogger,
      initialState: TRANSACTION_STATES.COMMITTING,
    });
    machine.transitionTo(TRANSACTION_STATES.ABORTABLE);
    machine.transitionTo(TRANSACTION_STATES.ABORTING);
    machine.transitionTo(TRANSACTION_STATES.FATAL_ERROR);

    for (const state of Object.values(TRANSACTION_STATES)) {
      expect(() => machine.transitionTo(state)).toThrow(FENCED_PRODUCER_MESSAGE);
    }
  });

  it('emits a transition event with from/to on every successful transition', () => {
    const machine = new TransactionStateMachine({ logger: silentLogger });
    const listener = vi.fn();
    machine.on('transition', listener);

    machine.transitionTo(TRANSACTION_STATES.READY);

    expect(listener).toHaveBeenCalledWith({ from: TRANSACTION_STATES.UNINITIALIZED, to: TRANSACTION_STATES.READY });
  });

  describe('illegalCallError', () => {
    it('names the call and the current state', () => {
      const machine = new TransactionStateMachine({ logger: silentLogger });

      expect(machine.illegalCallError('commit')).toEqual(
        new KafkaNonRetriableError('Transaction state exception: Cannot call "commit" in state "UNINITIALIZED"'),
      );
    });

    it('reports the fenced error from FATAL_ERROR', () => {
      const machine = new TransactionStateMachine({
        logger: silentLogger,
        initialState: TRANSACTION_STATES.FATAL_ERROR,
      });

      expect(machine.illegalCallError('commit')).toEqual(new KafkaNonRetriableError(FENCED_PRODUCER_MESSAGE));
    });
  });
});
