import { KafkaJSNonRetriableError } from '../errors.js';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter.js';
import type { InstrumentationEvent } from '../instrumentation/event.js';
import { createAclsApi } from './acls.js';
import { createConfigsApi } from './configs.js';
import { createGroupsApi } from './groups.js';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type AdminEventName } from './instrumentation-events.js';
import { createOffsetsApi } from './offsets.js';
import { createReassignmentsApi } from './reassignments.js';
import { createTopicsApi } from './topics.js';
import type { Admin, AdminOptions } from './types.js';

export type { Admin, AdminOptions, AclEntry, AclFilter, TopicConfig, TopicOffset } from './types.js';
export { events };

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `admin.events.${key}`)
  .join(', ');

/**
 * The user-facing admin client: topic/group/ACL/config/offset/reassignment operations plus
 * instrumentation events. Split by API group under `admin/`; this factory wires them together.
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

  const on = (
    eventName: AdminEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener => {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
    }

    return instrumentationEmitter.addListener(unwrap(eventName), (event: InstrumentationEvent<unknown>) => {
      const wrapped = { ...event, type: wrap(event.type) };
      Promise.resolve(listener(wrapped)).catch((error: unknown) => {
        const err = error as Error;
        logger.error(`Failed to execute listener: ${err.message}`, { eventName, stack: err.stack });
      });
    });
  };

  return {
    async connect(): Promise<void> {
      await cluster.connect();
      instrumentationEmitter.emit(CONNECT, {});
    },
    async disconnect(): Promise<void> {
      await cluster.disconnect();
      instrumentationEmitter.emit(DISCONNECT, {});
    },
    ...topics,
    ...offsets,
    ...configs,
    ...groups,
    ...acls,
    ...reassignments,
    on,
    logger: () => logger,
    events,
  };
}
