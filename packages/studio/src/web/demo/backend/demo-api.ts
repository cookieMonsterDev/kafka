import type { z } from 'zod';
import type { ClusterStatus, HealthResponse } from '../../lib/api';
import type { ProfilesResponse } from '../../lib/profiles-api';
import { scanRequestSchema } from '../../../shared/contracts/debug';
import {
  deleteGroupOffsetsRequestSchema,
  removeGroupMembersRequestSchema,
  resetGroupOffsetsRequestSchema,
} from '../../../shared/contracts/group';
import { deleteRecordsRequestSchema, messagesQuerySchema, tailQuerySchema } from '../../../shared/contracts/message';
import { burstRequestSchema, produceRequestSchema } from '../../../shared/contracts/produce';
import type { AclListResponse, QuotaListResponse, TransactionListResponse } from '../../../shared/contracts/settings';
import { terminalInputSchema } from '../../../shared/contracts/terminal';
import {
  alterTopicConfigsRequestSchema,
  createPartitionsRequestSchema,
  createTopicRequestSchema,
} from '../../../shared/contracts/topic';
import { DemoBurstJobs } from './burst-jobs';
import { DemoCluster, DemoError } from './demo-cluster';
import { DemoTerminals } from './demo-terminal';
import type { StreamEmit, StreamOpener } from './fake-event-source';
import { DEMO_PROFILE, SAMPLE_ACLS, SAMPLE_QUOTAS, SAMPLE_TRANSACTIONS } from './sample-data';

type Params = Readonly<Record<string, string>>;

interface DemoRequest {
  readonly params: Params;
  readonly url: URL;
  readonly body: unknown;
}

type RequestHandler = (request: DemoRequest) => Response;
type StreamHandler = (params: Params, url: URL, emit: StreamEmit, end: () => void) => (() => void) | null;

interface Route<Handler> {
  readonly method: string;
  readonly segments: readonly string[];
  readonly handler: Handler;
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
}

function errorResponse(status: number, code: string, message: string): Response {
  return json(status, { error: { code, message } });
}

function parse<T>(schema: z.ZodType<T>, value: unknown, what: string): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    throw new DemoError(400, 'bad_request', `invalid ${what}: ${parsed.error.issues[0]?.message ?? 'bad input'}`);
  }
  return parsed.data;
}

function param(params: Params, name: string): string {
  const value = params[name];
  if (value === undefined) throw new Error(`missing route param "${name}"`);
  return value;
}

function splitPath(pathname: string): string[] {
  return pathname.split('/').filter((segment) => segment.length > 0);
}

/** Positional segment matching with `:name` params, the same rules as `Router.match` in `src/server/router.ts`. */
function matchRoute<Handler>(
  routes: readonly Route<Handler>[],
  method: string,
  pathname: string,
): { readonly handler: Handler; readonly params: Params } | null {
  const segments = splitPath(pathname);
  for (const route of routes) {
    if (route.method !== method || route.segments.length !== segments.length) continue;
    const params: Record<string, string> = {};
    const matched = route.segments.every((segment, index) => {
      const actual = segments[index] ?? '';
      if (!segment.startsWith(':')) return segment === actual;
      params[segment.slice(1)] = decodeURIComponent(actual);
      return true;
    });
    if (matched) return { handler: route.handler, params };
  }
  return null;
}

/** Writes SSE frames the way `src/server/sse.ts` does, for the endpoints the UI reads with a streaming `fetch`. */
function eventStream(frames: readonly (readonly [event: string, data: unknown])[]): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const [event, data] of frames) {
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      }
      controller.close();
    },
  });
  return new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

/**
 * The studio HTTP API, answered in the page from a {@link DemoCluster}. Routes and payloads mirror
 * `src/server/routes/*`. Anything the demo cannot simulate (Schema Registry, replay, scenarios,
 * profile comparison, partition lookup, profile switching) answers 501 and says why.
 */
export class DemoApi {
  readonly cluster = new DemoCluster();
  private readonly bursts = new DemoBurstJobs(this.cluster);
  private readonly terminals = new DemoTerminals(this.cluster);
  private readonly requestRoutes: Route<RequestHandler>[] = [];
  private readonly streamRoutes: Route<StreamHandler>[] = [];

  constructor() {
    this.registerRequestRoutes();
    this.registerStreamRoutes();
  }

  handle(method: string, url: URL, body: unknown): Response {
    const matched = matchRoute(this.requestRoutes, method.toUpperCase(), url.pathname);
    if (matched === null) {
      return errorResponse(
        501,
        'not_in_demo',
        'This feature is not available in the browser demo. Install Studio to use it against your own cluster.',
      );
    }
    try {
      return matched.handler({ params: matched.params, url, body });
    } catch (error) {
      if (error instanceof DemoError) return errorResponse(error.status, error.code, error.message);
      throw error;
    }
  }

  /** Opens the server-sent event streams the UI reads with `EventSource`. `null` when none exists at that path. */
  readonly openStream: StreamOpener = (url, emit, end) => {
    const matched = matchRoute(this.streamRoutes, 'GET', url.pathname);
    return matched === null ? null : matched.handler(matched.params, url, emit, end);
  };

  private request(method: string, pattern: string, handler: RequestHandler): void {
    this.requestRoutes.push({ method, segments: splitPath(pattern), handler });
  }

  private stream(pattern: string, handler: StreamHandler): void {
    this.streamRoutes.push({ method: 'GET', segments: splitPath(pattern), handler });
  }

  private registerStreamRoutes(): void {
    const { cluster } = this;

    this.stream('/api/events', (_params, _url, emit) => cluster.onActivity((event) => emit('activity', event)));
    this.stream('/api/topics/:name/tail', (params, url, emit, end) => {
      const topic = param(params, 'name');
      const query = tailQuerySchema.safeParse(Object.fromEntries(url.searchParams));
      // The real server answers an invalid query with a 400, which an EventSource only sees as an error.
      if (!query.success) return null;
      const { partition } = query.data;
      try {
        cluster.assertTopicExists(topic);
      } catch (error) {
        emit('tail-error', { message: error instanceof Error ? error.message : 'tail failed' });
        end();
        return () => {};
      }
      return cluster.onRecord((recordTopic, record) => {
        if (recordTopic !== topic || (partition !== undefined && record.partition !== partition)) return;
        emit('message', record);
        // The real server reports its own tailing to the board's activity feed, so the demo does too.
        cluster.publishActivity({ kind: 'consume', topic, partition: record.partition, count: 1, bytes: record.size });
      });
    });
    this.stream('/api/produce/burst/:jobId', (params, _url, emit, end) =>
      this.bursts.watch(param(params, 'jobId'), (progress) => emit('progress', progress), end),
    );
    this.stream('/api/terminal/sessions/:id/events', (params, _url, emit, end) =>
      this.terminals.attach(param(params, 'id'), emit, end),
    );
  }

  private registerRequestRoutes(): void {
    const { cluster } = this;

    this.request('GET', '/api/health', () =>
      json(200, {
        status: 'ok',
        readOnly: false,
        uptimeSeconds: Math.floor((Date.now() - cluster.startedAt) / 1000),
      } satisfies HealthResponse),
    );
    this.request('GET', '/api/cluster', () =>
      json(200, {
        connected: true,
        brokerCount: cluster.brokerCount(),
        clusterId: cluster.clusterId,
      } satisfies ClusterStatus),
    );
    this.request('GET', '/api/cluster/log-dirs', () => json(200, cluster.logDirs()));
    this.request('GET', '/api/profiles', () =>
      json(200, {
        active: DEMO_PROFILE,
        profiles: { [DEMO_PROFILE]: { clientId: 'kafka-studio', brokers: ['localhost:9092'] } },
      } satisfies ProfilesResponse),
    );

    this.request('GET', '/api/topics', () => json(200, cluster.listTopics()));
    this.request('POST', '/api/topics', ({ body }) => {
      const request = parse(createTopicRequestSchema, body, 'create-topic request');
      cluster.createTopic(request);
      return json(201, { topic: request.topic });
    });
    this.request('GET', '/api/topics/:name', ({ params }) => json(200, cluster.topicDetail(param(params, 'name'))));
    this.request('DELETE', '/api/topics/:name', ({ params }) => {
      cluster.deleteTopic(param(params, 'name'));
      return new Response(null, { status: 204 });
    });
    this.request('POST', '/api/topics/:name/partitions', ({ params, body }) => {
      const name = param(params, 'name');
      const { count } = parse(createPartitionsRequestSchema, body, 'add-partitions request');
      cluster.addPartitions(name, count);
      return json(200, { topic: name, count });
    });
    this.request('PATCH', '/api/topics/:name/configs', ({ params, body }) => {
      const name = param(params, 'name');
      cluster.alterConfigs(name, parse(alterTopicConfigsRequestSchema, body, 'config-update request'));
      return json(200, { topic: name });
    });
    this.request('GET', '/api/topics/:name/messages', ({ params, url }) => {
      const query = parse(messagesQuerySchema, Object.fromEntries(url.searchParams), 'messages query');
      return json(200, cluster.readMessages(param(params, 'name'), query));
    });
    this.request('POST', '/api/topics/:name/records/delete', ({ params, body }) => {
      const name = param(params, 'name');
      cluster.deleteRecords(name, parse(deleteRecordsRequestSchema, body, 'delete-records request'));
      return json(200, { topic: name });
    });

    this.request('POST', '/api/produce', ({ body }) =>
      json(200, cluster.produce(parse(produceRequestSchema, body, 'produce request'), 'studio')),
    );
    this.request('POST', '/api/produce/burst', ({ body }) => {
      const request = parse(burstRequestSchema, body, 'burst request');
      cluster.assertTopicExists(request.topic);
      return json(202, { jobId: this.bursts.start(request) });
    });
    this.request('DELETE', '/api/produce/burst/:jobId', ({ params }) => {
      const jobId = param(params, 'jobId');
      if (!this.bursts.cancel(jobId)) throw new DemoError(404, 'unknown_job', `unknown burst job "${jobId}"`);
      return json(202, { jobId });
    });

    this.request('GET', '/api/groups', () => json(200, cluster.listGroups()));
    this.request('GET', '/api/groups/:id', ({ params }) => json(200, cluster.groupDetail(param(params, 'id'))));
    this.request('POST', '/api/groups/:id/offsets/reset', ({ params, body }) =>
      json(
        200,
        cluster.resetOffsets(param(params, 'id'), parse(resetGroupOffsetsRequestSchema, body, 'offset reset request')),
      ),
    );
    this.request('DELETE', '/api/groups/:id/offsets', ({ params, body }) =>
      json(
        200,
        cluster.deleteOffsets(
          param(params, 'id'),
          parse(deleteGroupOffsetsRequestSchema, body, 'delete offsets request'),
        ),
      ),
    );
    this.request('POST', '/api/groups/:id/members/remove', ({ params, body }) =>
      json(
        200,
        cluster.removeMembers(
          param(params, 'id'),
          parse(removeGroupMembersRequestSchema, body, 'remove members request'),
        ),
      ),
    );
    this.request('DELETE', '/api/groups/:id', ({ params }) => {
      cluster.deleteGroup(param(params, 'id'));
      return new Response(null, { status: 204 });
    });
    this.request('GET', '/api/share-groups', () => json(200, cluster.listShareGroups()));
    this.request('GET', '/api/share-groups/:id', ({ params }) =>
      json(200, cluster.shareGroupDetail(param(params, 'id'))),
    );

    this.request('GET', '/api/acls', () => json(200, { acls: SAMPLE_ACLS } satisfies AclListResponse));
    this.request('GET', '/api/quotas', () => json(200, { quotas: SAMPLE_QUOTAS } satisfies QuotaListResponse));
    this.request('GET', '/api/transactions', () =>
      json(200, { transactions: SAMPLE_TRANSACTIONS } satisfies TransactionListResponse),
    );

    this.request('POST', '/api/debug/scan', ({ body }) => {
      const result = cluster.scan(parse(scanRequestSchema, body, 'scan request'));
      return eventStream([
        ['progress', { scanned: result.scanned, matched: result.matched, coverage: result.coverage }],
        ['result', result],
      ]);
    });

    this.request('POST', '/api/terminal/sessions', () =>
      json(201, { id: this.terminals.create(), profile: DEMO_PROFILE, readOnly: false }),
    );
    this.request('POST', '/api/terminal/sessions/:id/input', ({ params, body }) => {
      const { data } = parse(terminalInputSchema, body, 'terminal input');
      if (!this.terminals.input(param(params, 'id'), data)) {
        throw new DemoError(404, 'session_closed', 'Terminal session has ended.');
      }
      return json(200, { ok: true });
    });
    this.request('POST', '/api/terminal/sessions/:id/resize', () => json(200, { ok: true }));
    this.request('DELETE', '/api/terminal/sessions/:id', ({ params }) => {
      this.terminals.close(param(params, 'id'));
      return json(200, { ok: true });
    });
  }
}
