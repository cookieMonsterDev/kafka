import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../broker/index';
import { KafkaNonRetriableError, KafkaNumberOfRetriesExceeded } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { retrier } from '../retry/index';
import { COORDINATOR_LOADING_ERRORS } from './error-classifier';
import { CONTROLLER, coordinatorTarget, nodeTarget, runOnTargets, type BrokerTarget, type RunContext } from './run';

function errorOfType(type: string) {
  return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
}

function fakeBroker(nodeId: number): Broker {
  return { nodeId } as unknown as Broker;
}

function fakeContext(resolve: (target: BrokerTarget) => Broker = () => fakeBroker(1)) {
  const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
  const warn = vi.spyOn(logger, 'warn');
  const resolveTarget = vi.fn(async (target: BrokerTarget) => resolve(target));
  const context = {
    logger,
    retrier: retrier({ retries: 2, initialRetryTime: 1, maxRetryTime: 5 }),
    resolve: resolveTarget,
    resolveAll: vi.fn(async (targets: readonly BrokerTarget[]) => Promise.all(targets.map(resolveTarget))),
    refreshMetadata: vi.fn(async () => undefined),
    recover: vi.fn(async (_error: unknown) => undefined),
  } satisfies RunContext;
  return { context, warn };
}

describe('cluster/runOnTargets', () => {
  it('runs the work on the resolved target', async () => {
    const broker = fakeBroker(3);
    const { context } = fakeContext(() => broker);

    await expect(runOnTargets(context, CONTROLLER, async (resolved) => resolved)).resolves.toBe(broker);
    expect(context.resolve).toHaveBeenCalledWith(CONTROLLER);
    expect(context.refreshMetadata).not.toHaveBeenCalled();
  });

  it('recovers, resolves the target again and retries when the target moved', async () => {
    const { context, warn } = fakeContext();
    const notController = errorOfType('NOT_CONTROLLER');
    const work = vi.fn().mockRejectedValueOnce(notController).mockResolvedValue('done');

    await expect(runOnTargets(context, CONTROLLER, work, { description: 'Could not do it' })).resolves.toBe('done');
    expect(work).toHaveBeenCalledTimes(2);
    expect(context.resolve).toHaveBeenCalledTimes(2);
    expect(context.recover).toHaveBeenCalledExactlyOnceWith(notController);
    expect(warn).toHaveBeenCalledWith('Could not do it', expect.objectContaining({ retryCount: 0 }));
  });

  it('rejects with any other error as is, without recovering', async () => {
    const { context } = fakeContext();
    const error = errorOfType('NOT_LEADER_OR_FOLLOWER');
    const work = vi.fn().mockRejectedValue(error);

    await expect(runOnTargets(context, CONTROLLER, work)).rejects.toBe(error);
    expect(work).toHaveBeenCalledTimes(1);
    expect(context.recover).not.toHaveBeenCalled();
  });

  it('retries an error type listed in retryOn', async () => {
    const { context } = fakeContext();
    const work = vi.fn().mockRejectedValueOnce(errorOfType('GROUP_LOAD_IN_PROGRESS')).mockResolvedValue('done');

    await expect(
      runOnTargets(context, coordinatorTarget('group'), work, { retryOn: COORDINATOR_LOADING_ERRORS }),
    ).resolves.toBe('done');
    expect(work).toHaveBeenCalledTimes(2);
  });

  it('gives up once the retries run out', async () => {
    const { context } = fakeContext();
    const work = vi.fn().mockRejectedValue(errorOfType('NOT_CONTROLLER'));

    await expect(runOnTargets(context, CONTROLLER, work)).rejects.toBeInstanceOf(KafkaNumberOfRetriesExceeded);
    expect(work).toHaveBeenCalledTimes(3);
  });

  it('retries the original error when the recovery fails, logging the recovery failure', async () => {
    const { context, warn } = fakeContext();
    context.recover.mockRejectedValueOnce(new KafkaNonRetriableError('metadata refresh failed'));
    const work = vi.fn().mockRejectedValueOnce(errorOfType('NOT_CONTROLLER')).mockResolvedValue('done');

    await expect(runOnTargets(context, CONTROLLER, work)).resolves.toBe('done');
    expect(work).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenCalledWith(
      'Recovery before the retry failed',
      expect.objectContaining({ recoveryError: 'metadata refresh failed', retryCount: 0 }),
    );
  });

  it('rejects with the original error, not the failed recovery, once the retries run out', async () => {
    const { context } = fakeContext();
    context.recover.mockRejectedValue(new Error('metadata refresh failed'));
    const notController = errorOfType('NOT_CONTROLLER');
    const work = vi.fn().mockRejectedValue(notController);

    await expect(runOnTargets(context, CONTROLLER, work)).rejects.toMatchObject({
      name: 'KafkaNumberOfRetriesExceeded',
      message: notController.message,
    });
    expect(work).toHaveBeenCalledTimes(3);
  });

  it('refreshes metadata before every attempt when asked to', async () => {
    const { context } = fakeContext();
    const work = vi.fn().mockRejectedValueOnce(errorOfType('NOT_CONTROLLER')).mockResolvedValue('done');

    await runOnTargets(context, CONTROLLER, work, { refreshMetadata: true });
    expect(context.refreshMetadata).toHaveBeenCalledTimes(2);
  });

  it('uses the given retrier instead of the context one', async () => {
    const { context } = fakeContext();
    const work = vi.fn().mockRejectedValue(errorOfType('NOT_CONTROLLER'));

    await expect(
      runOnTargets(context, CONTROLLER, work, { retrier: retrier({ retries: 0, initialRetryTime: 1 }) }),
    ).rejects.toBeInstanceOf(KafkaNumberOfRetriesExceeded);
    expect(work).toHaveBeenCalledTimes(1);
  });

  describe('with retryRetriable', () => {
    it('retries any retriable error after recovering', async () => {
      const { context } = fakeContext();
      const error = errorOfType('NOT_LEADER_OR_FOLLOWER');
      const work = vi.fn().mockRejectedValueOnce(error).mockResolvedValue('done');

      await expect(runOnTargets(context, () => [], work, { retryRetriable: true })).resolves.toBe('done');
      expect(context.recover).toHaveBeenCalledExactlyOnceWith(error);
    });

    it('leaves a non-retriable error to the retrier, without recovering', async () => {
      const { context } = fakeContext();
      const work = vi.fn().mockRejectedValue(new KafkaNonRetriableError('invalid'));

      await expect(runOnTargets(context, () => [], work, { retryRetriable: true })).rejects.toBeInstanceOf(
        KafkaNonRetriableError,
      );
      expect(work).toHaveBeenCalledTimes(1);
      expect(context.recover).not.toHaveBeenCalled();
    });
  });

  describe('with a target list', () => {
    it('reads the targets on every attempt and passes one broker per target', async () => {
      const brokers: Record<string, Broker> = { '1': fakeBroker(1), '2': fakeBroker(2) };
      const { context } = fakeContext((target) => brokers[String((target as { nodeId: string }).nodeId)]!);
      let nodeIds = ['1'];
      const work = vi.fn(async (resolved: Broker[]) => {
        if (resolved.length === 1) {
          nodeIds = ['1', '2'];
          throw errorOfType('NOT_CONTROLLER');
        }
        return resolved;
      });

      await expect(
        runOnTargets(context, () => nodeIds.map((nodeId) => nodeTarget(nodeId)), work, {
          retryOn: new Set(['NOT_CONTROLLER']),
        }),
      ).resolves.toEqual([brokers['1'], brokers['2']]);
      expect(work).toHaveBeenCalledTimes(2);
    });

    it('resolves the whole list in one call, so coordinators can share one lookup', async () => {
      const { context } = fakeContext();
      const targets = [coordinatorTarget('a'), coordinatorTarget('b')];

      await runOnTargets(
        context,
        () => targets,
        async (brokers) => brokers,
      );
      expect(context.resolveAll).toHaveBeenCalledTimes(1);
      expect(context.resolveAll).toHaveBeenCalledWith(targets);
    });

    it('retries when the error moved any of the targets', async () => {
      const { context } = fakeContext();
      const work = vi.fn().mockRejectedValueOnce(errorOfType('NOT_COORDINATOR_FOR_GROUP')).mockResolvedValue('done');

      await expect(runOnTargets(context, () => [nodeTarget(1), coordinatorTarget('group')], work)).resolves.toBe(
        'done',
      );
    });

    it('retries nothing by kind for an empty list', async () => {
      const { context } = fakeContext();
      const error = errorOfType('NOT_CONTROLLER');

      await expect(runOnTargets(context, () => [], vi.fn().mockRejectedValue(error))).rejects.toBe(error);
      expect(context.resolve).not.toHaveBeenCalled();
    });
  });
});
