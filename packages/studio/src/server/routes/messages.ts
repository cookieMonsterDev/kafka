import type { MessageRecord } from '../../shared/contracts/message';
import {
  deleteRecordsRequestSchema,
  messagesQuerySchema,
  seekByTimeRequestSchema,
  tailQuerySchema,
  type SeekByTimeResponse,
} from '../../shared/contracts/message';
import { sendError, sendJson } from '../create-server';
import type { AdminPool } from '../kafka/admin-pool';
import type { StudioEventBus } from '../kafka/events';
import type { MessageConsumerFactory } from '../kafka/messages';
import { readMessagesPage } from '../kafka/messages';
import { runTail } from '../kafka/tail';
import { readJsonBody } from '../json';
import { requireParam, type Router } from '../router';
import { openSseStream, type SseStream } from '../sse';

export interface MessagesRouteContext {
  readonly pool: AdminPool;
  readonly consumerFactory: MessageConsumerFactory;
  /** Caps how many undelivered tail messages one SSE connection buffers — see `kafka/tail.ts`. */
  readonly maxTail: number;
  readonly events: StudioEventBus;
  getActiveProfile(): string | null;
}

function messageBytes(message: MessageRecord): number {
  return (message.key?.length ?? 0) + (message.value?.length ?? 0);
}

function queryParams(url: URL): Record<string, string> {
  const params: Record<string, string> = {};
  for (const [key, value] of url.searchParams) params[key] = value;
  return params;
}

/** `GET .../messages` is a bounded snapshot read; `GET .../tail` streams new messages over SSE from the moment it opens. Neither replays into the other. */
export function registerMessageRoutes(router: Router, context: MessagesRouteContext): void {
  router.get('/api/topics/:name/messages', async (_req, res, params, url) => {
    const name = requireParam(params, 'name');
    const parsed = messagesQuerySchema.safeParse(queryParams(url));
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid messages query', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const consumer = context.consumerFactory(context.getActiveProfile()).consumer();
    const response = await readMessagesPage(admin, consumer, { topic: name, query: parsed.data });
    sendJson(res, 200, response);
  });

  router.get('/api/topics/:name/tail', async (req, res, params, url) => {
    const name = requireParam(params, 'name');
    const parsed = tailQuerySchema.safeParse(queryParams(url));
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid tail query', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const consumer = context.consumerFactory(context.getActiveProfile()).consumer();
    const controller = new AbortController();
    const stream = openSseStream(req, res, () => controller.abort());
    // Forwards every delivered frame to the activity firehose too — the board's particle layer is
    // this studio's own tailing, not a separate signal.
    const observedStream: Pick<SseStream, 'send'> = {
      send(event, data) {
        stream.send(event, data);
        if (event === 'message') {
          const message = data as MessageRecord;
          context.events.publish({
            kind: 'consume',
            topic: name,
            partition: message.partition,
            count: 1,
            bytes: messageBytes(message),
          });
        }
      },
    };

    try {
      await runTail(
        admin,
        consumer,
        observedStream,
        { topic: name, partition: parsed.data.partition, maxBuffered: context.maxTail },
        controller.signal,
      );
    } catch (error) {
      // Named `tail-error`, not `error` — `EventSource` fires its own native `error` on every
      // connection drop, and a same-named custom event would be indistinguishable from that.
      stream.send('tail-error', { message: error instanceof Error ? error.message : 'tail failed' });
    } finally {
      stream.close();
    }
  });

  router.post('/api/topics/:name/offsets/by-time', async (req, res, params) => {
    const name = requireParam(params, 'name');
    const body = await readJsonBody(req);
    const parsed = seekByTimeRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid seek-by-time request', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const entries = await admin.fetchTopicOffsetsByTimestamp(name, parsed.data.timestamp);
    const wanted = parsed.data.partition;
    const filtered = wanted === undefined ? entries : entries.filter((entry) => entry.partition === wanted);

    const response: SeekByTimeResponse = {
      offsets: filtered.map((entry) => ({
        partition: entry.partition,
        offset: entry.offset < 0n ? null : entry.offset.toString(),
      })),
    };
    sendJson(res, 200, response);
  });

  router.post('/api/topics/:name/records/delete', async (req, res, params) => {
    const name = requireParam(params, 'name');
    const body = await readJsonBody(req);
    const parsed = deleteRecordsRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid delete-records request', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    await admin.deleteTopicRecords({
      topic: name,
      partitions: parsed.data.partitions.map((entry) => ({
        partition: entry.partition,
        offset: BigInt(entry.beforeOffset),
      })),
    });
    sendJson(res, 200, { topic: name });
  });
}
