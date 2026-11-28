import { vi } from 'vitest';
import type { Broker } from '../broker/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import type { CoordinatorType } from '../protocol/enums/coordinator-types';
import { retrier } from '../retry/index';
import { FAST_RETRY_DEFAULTS } from '../retry/test-defaults';
import { runOnTargets, type BrokerTarget, type RunContext, type RunOptions, type Targets } from './run';

type FakeBroker = object;

/** The fake broker each kind of target resolves to. A function receives the target's fields. */
export interface FakeTargets {
  controller?: FakeBroker;
  coordinator?: FakeBroker | ((key: string, coordinatorType: CoordinatorType | undefined) => FakeBroker);
  leader?: FakeBroker | ((topic: string, partition: number) => FakeBroker);
  node?: FakeBroker | ((nodeId: string) => FakeBroker);
  any?: FakeBroker;
}

function pick<Args extends unknown[]>(
  entry: FakeBroker | ((...args: Args) => FakeBroker) | undefined,
  kind: string,
  ...args: Args
): FakeBroker {
  if (entry === undefined) throw new Error(`The fake cluster resolves no ${kind} target`);
  return typeof entry === 'function' ? (entry as (...args: Args) => FakeBroker)(...args) : entry;
}

function resolveFake(targets: FakeTargets, target: BrokerTarget): FakeBroker {
  switch (target.kind) {
    case 'controller':
      return pick(targets.controller, 'controller');
    case 'coordinator':
      return pick(targets.coordinator, 'coordinator', target.key, target.coordinatorType);
    case 'leader':
      return pick(targets.leader, 'leader', target.topic, target.partition);
    case 'node':
      return pick(targets.node, 'node', String(target.nodeId));
    case 'any':
      return pick(targets.any, 'any');
  }
}

/**
 * A unit-test cluster whose `run` keeps the real retries and error classification but resolves
 * targets to the given fake brokers instead of reading metadata. `resolveTarget`, `refreshMetadata`
 * and `recover` are mocks a test can assert on. `extra` adds any other cluster members the code
 * under test reads. Not built into the package (`tsconfig.build.json`).
 */
export function fakeRunCluster<Extra extends object = Record<never, never>>(
  targets: FakeTargets,
  extra: Extra = {} as Extra,
) {
  const resolveTarget = vi.fn(async (target: BrokerTarget) => resolveFake(targets, target) as Broker);
  const refreshMetadata = vi.fn(async () => undefined);
  const recover = vi.fn(async (_error: unknown) => undefined);
  const context: RunContext = {
    logger: createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} }),
    retrier: retrier(FAST_RETRY_DEFAULTS),
    resolve: resolveTarget,
    resolveAll: (runTargets) => Promise.all(runTargets.map((target) => resolveTarget(target))),
    refreshMetadata,
    recover,
  };
  const run = vi.fn(
    (
      runTargets: Targets,
      work: ((broker: Broker) => Promise<unknown>) | ((brokers: Broker[]) => Promise<unknown>),
      options?: RunOptions,
    ) => runOnTargets(context, runTargets, work, options),
  );

  return Object.assign({ run, resolveTarget, refreshMetadata, recover }, extra);
}
