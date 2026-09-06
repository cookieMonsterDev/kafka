import type { ProduceMessage } from '../../shared/contracts/produce';
import { burstRequestSchema, produceRequestSchema } from '../../shared/contracts/produce';
import { sendError, sendJson } from '../create-server';
import type { StudioEventBus } from '../kafka/events';
import { BurstJobManager, ProducerPool, sendMessages } from '../kafka/produce';
import { readJsonBody } from '../json';
import { requireParam, type Router } from '../router';
import { openSseStream } from '../sse';

export interface ProduceRouteContext {
  readonly producers: ProducerPool;
  readonly jobs: BurstJobManager;
  readonly events: StudioEventBus;
  getActiveProfile(): string | null;
}

function messageBytes(message: ProduceMessage): number {
  return (message.key?.length ?? 0) + (message.value?.length ?? 0);
}

/**
 * `POST /api/produce` sends messages inline and waits for the broker's acknowledgment. A burst
 * (`POST /api/produce/burst`) instead starts a background job and returns immediately with a
 * `jobId` — the caller follows its progress over `GET .../burst/:jobId` (SSE) and can stop it early
 * with `DELETE .../burst/:jobId`. A finished job's final state stays queryable for a short grace
 * period (`BurstJobManager`'s retention window) so a client reconnecting right after completion
 * still sees it.
 */
export function registerProduceRoutes(router: Router, context: ProduceRouteContext): void {
  router.post('/api/produce', async (req, res) => {
    const body = await readJsonBody(req);
    const parsed = produceRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid produce request', { issues: parsed.error.issues });
      return;
    }

    const producer = await context.producers.get(context.getActiveProfile());
    const response = await sendMessages(producer, parsed.data);
    sendJson(res, 200, response);

    const partitions = [...new Set(response.results.map((entry) => entry.partition))];
    context.events.publish({
      kind: 'produce',
      topic: parsed.data.topic,
      partition: partitions.length === 1 ? (partitions[0] ?? null) : null,
      count: parsed.data.messages.length,
      bytes: parsed.data.messages.reduce((total, message) => total + messageBytes(message), 0),
    });
  });

  router.post('/api/produce/burst', async (req, res) => {
    const body = await readJsonBody(req);
    const parsed = burstRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid burst request', { issues: parsed.error.issues });
      return;
    }

    const producer = await context.producers.get(context.getActiveProfile());
    const job = context.jobs.start(producer, parsed.data);
    sendJson(res, 202, { jobId: job.id });
  });

  router.get('/api/produce/burst/:jobId', (req, res, params) => {
    const jobId = requireParam(params, 'jobId');
    const job = context.jobs.get(jobId);
    if (job === undefined) {
      sendError(res, 404, 'unknown_job', `unknown burst job "${jobId}"`);
      return;
    }

    let unsubscribe: (() => void) | undefined;
    const stream = openSseStream(req, res, () => unsubscribe?.());

    const initial = job.snapshot();
    stream.send('progress', initial);
    if (initial.status === 'running') {
      unsubscribe = job.onProgress((progress) => {
        stream.send('progress', progress);
        if (progress.status !== 'running') stream.close();
      });
    } else {
      stream.close();
    }
  });

  router.delete('/api/produce/burst/:jobId', (_req, res, params) => {
    const jobId = requireParam(params, 'jobId');
    if (!context.jobs.cancel(jobId)) {
      sendError(res, 404, 'unknown_job', `unknown burst job "${jobId}"`);
      return;
    }
    sendJson(res, 202, { jobId });
  });
}
