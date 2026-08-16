import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { createAclsApi } from './acls';
import { createConfigsApi } from './configs';
import { createGroupsApi } from './groups';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type AdminEventName } from './instrumentation-events';
import { createOffsetsApi } from './offsets';
import { createReassignmentsApi } from './reassignments';
import { createScramApi } from './scram';
import { createTopicsApi } from './topics';
import type { Admin, AdminOptions } from './types';

export type { Admin, AdminOptions, AclEntry, AclFilter, TopicConfig, TopicOffset } from './types';
export { events };

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `admin.events.${key}`)
  .join(', ');

/**
 * User-facing admin client: topics, groups, ACLs, configs, offsets, and reassignments.
 *
 * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
 */
export function createAdmin({
  cluster,
  logger: rootLogger,
  retry,
  instrumentationEmitter: rootInstrumentationEmitter,
}: AdminOptions): Admin {
  const logger = rootLogger.namespace('Admin');
  const instrumentationEmitter = rootInstrumentationEmitter ?? new InstrumentationEventEmitter();
  const context = { cluster, logger, rootLogger, retry };

  const offsets = createOffsetsApi(context);
  const topics = createTopicsApi(context, { fetchTopicOffsets: offsets.fetchTopicOffsets });
  const configs = createConfigsApi(context);
  const groups = createGroupsApi(context);
  const acls = createAclsApi(context);
  const reassignments = createReassignmentsApi(context);
  const scram = createScramApi(context);

  const on = (
    eventName: AdminEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener => {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
    }

    return instrumentationEmitter.addListener(unwrap(eventName), (event: InstrumentationEvent<unknown>) => {
      const wrapped = { ...event, type: wrap(event.type) };
      Promise.resolve(listener(wrapped)).catch((error: unknown) => {
        const err = error as Error;
        logger.error(`Failed to execute listener: ${err.message}`, { eventName, stack: err.stack });
      });
    });
  };

  async function connect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
    instrumentationEmitter.emit(CONNECT, {});
  }

  async function disconnect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.disconnect(), signal);
    instrumentationEmitter.emit(DISCONNECT, {});
  }

  return {
    connect,
    disconnect,
    ...topics,
    ...offsets,
    ...configs,
    ...groups,
    ...acls,
    ...reassignments,
    ...scram,
    on,
    logger: () => logger,
    events,
    [Symbol.asyncDispose]: disconnect,
  };
}
