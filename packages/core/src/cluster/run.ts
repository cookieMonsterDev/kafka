import type { Broker } from '../broker/index';
import type { Logger } from '../loggers/index';
import type { CoordinatorType } from '../protocol/enums/coordinator-types';
import { isErrorRetriable, type Retrier } from '../retry/index';
import { isRetriableFor, type TargetKind } from './error-classifier';

/** The broker a request goes to, resolved again on every attempt. */
export type BrokerTarget =
  | { readonly kind: 'controller' }
  | { readonly kind: 'coordinator'; readonly key: string; readonly coordinatorType?: CoordinatorType }
  | { readonly kind: 'leader'; readonly topic: string; readonly partition: number }
  | { readonly kind: 'node'; readonly nodeId: string | number }
  | { readonly kind: 'any' };

export const CONTROLLER: BrokerTarget = Object.freeze({ kind: 'controller' });
export const ANY_BROKER: BrokerTarget = Object.freeze({ kind: 'any' });

/** The coordinator of `key`: a group id, or a transactional id with `COORDINATOR_TYPES.TRANSACTION`. */
export function coordinatorTarget(key: string, coordinatorType?: CoordinatorType): BrokerTarget {
  return { kind: 'coordinator', key, coordinatorType };
}

export function leaderTarget(topic: string, partition: number): BrokerTarget {
  return { kind: 'leader', topic, partition };
}

export function nodeTarget(nodeId: string | number): BrokerTarget {
  return { kind: 'node', nodeId };
}

export interface RunOptions {
  /** Retries with this retrier instead of the cluster's, for a client with its own retry options. */
  retrier?: Retrier;
  /**
   * Error types worth another attempt besides a moved target, such as `COORDINATOR_LOADING_ERRORS`
   * from `cluster/error-classifier.ts`.
   */
  retryOn?: ReadonlySet<string>;
  /**
   * Retries every error the retrier treats as retriable, not only a moved target and `retryOn`, so
   * `retryOn` has no effect with it.
   */
  retryRetriable?: boolean;
  /** Refreshes metadata before every attempt, so the target is resolved from current metadata. */
  refreshMetadata?: boolean;
  /** Logged as a warning before each retry; without it, retries are logged at debug level. */
  description?: string;
}

/** What {@link runOnTargets} needs from a cluster. Unit tests pass a fake to stub target resolution. */
export interface RunContext {
  logger: Logger;
  retrier: Retrier;
  resolve(target: BrokerTarget): Promise<Broker>;
  refreshMetadata(): Promise<void>;
  /** Carries out the recovery the error classifier picks for `error`. */
  recover(error: unknown): Promise<void>;
}

export type Targets = BrokerTarget | (() => readonly BrokerTarget[]);

/**
 * Resolves `targets`, runs `work` on the resolved brokers, and retries. Each attempt resolves the
 * targets again. An error is retried when the classifier says the target moved or its type is in
 * `retryOn`, after the recovery the classifier picks for it; any other error rejects as is.
 *
 * With a function, `targets` is read on every attempt (after the optional metadata refresh) and
 * `work` gets the brokers in the same order.
 */
export function runOnTargets<T>(
  context: RunContext,
  targets: BrokerTarget,
  work: (broker: Broker) => Promise<T>,
  options?: RunOptions,
): Promise<T>;
export function runOnTargets<T>(
  context: RunContext,
  targets: () => readonly BrokerTarget[],
  work: (brokers: Broker[]) => Promise<T>,
  options?: RunOptions,
): Promise<T>;
export function runOnTargets<T>(
  context: RunContext,
  targets: Targets,
  work: ((broker: Broker) => Promise<T>) | ((brokers: Broker[]) => Promise<T>),
  options?: RunOptions,
): Promise<T>;
export function runOnTargets<T>(
  context: RunContext,
  targets: Targets,
  work: ((broker: Broker) => Promise<T>) | ((brokers: Broker[]) => Promise<T>),
  { retrier, retryOn, retryRetriable = false, refreshMetadata = false, description }: RunOptions = {},
): Promise<T> {
  const retry = retrier ?? context.retrier;

  return retry(async (bail, retryCount, retryTime) => {
    let kinds: readonly TargetKind[] = typeof targets === 'function' ? [] : [targets.kind];
    try {
      if (refreshMetadata) await context.refreshMetadata();

      if (typeof targets === 'function') {
        const resolvedTargets = targets();
        kinds = resolvedTargets.map(({ kind }) => kind);
        const brokers = await Promise.all(resolvedTargets.map((target) => context.resolve(target)));
        return await (work as (brokers: Broker[]) => Promise<T>)(brokers);
      }

      return await (work as (broker: Broker) => Promise<T>)(await context.resolve(targets));
    } catch (error) {
      if (retryRetriable) {
        // The retrier rejects a non-retriable error itself, as it would without `run`.
        if (!isErrorRetriable(error as Error)) throw error;
      } else if (!isRetriableFor(error, kinds, retryOn)) {
        bail(error as Error);
        // `bail` already rejected the run; the retrier ignores this attempt's result.
        return undefined as T;
      }

      const details = { error: error instanceof Error ? error.message : String(error), retryCount, retryTime };
      if (description) {
        context.logger.warn(description, details);
      } else {
        context.logger.debug('Request failed, retrying', details);
      }
      await context.recover(error);
      throw error;
    }
  });
}
