import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { ProfileClientPool } from './profile-client-pool';
import type { Message, Producer } from '@cookiemonsterdev/kafka-core';
import type {
  BurstJobStatus,
  BurstProgress,
  BurstRequest,
  ProduceMessage,
  ProduceResponse,
} from '../../shared/contracts/produce';

export type PooledProducer = Pick<Producer, 'connect' | 'disconnect' | 'send'>;

/** Everything {@link ProducerPool} needs to build a fresh, unconnected producer for one profile. */
export type ProducerClientFactory = (profileName: string | null) => { producer(): PooledProducer };

/** A profile-scoped pool retaining the producer's typed capabilities. */
export class ProducerPool extends ProfileClientPool<PooledProducer> {
  constructor(createClient: ProducerClientFactory) {
    super((profile) => createClient(profile).producer());
  }
}

function toCoreMessage(message: ProduceMessage): Message {
  return {
    value: message.value,
    ...(message.key !== undefined ? { key: message.key } : {}),
    ...(message.partition !== undefined ? { partition: message.partition } : {}),
    ...(message.headers !== undefined ? { headers: message.headers } : {}),
    ...(message.timestamp !== undefined ? { timestamp: message.timestamp } : {}),
  };
}

export interface SendMessagesInput {
  readonly topic: string;
  readonly messages: readonly ProduceMessage[];
  readonly acks?: number;
  readonly signal?: AbortSignal;
}

export async function sendMessages(producer: PooledProducer, input: SendMessagesInput): Promise<ProduceResponse> {
  const metadata = await producer.send({
    topic: input.topic,
    messages: input.messages.map(toCoreMessage),
    ...(input.acks !== undefined ? { acks: input.acks } : {}),
    ...(input.signal !== undefined ? { signal: input.signal } : {}),
  });
  return {
    results: metadata.map((entry) => ({ partition: entry.partition, offset: entry.baseOffset.toString() })),
  };
}

/** Replaces every `{{seq}}` token in `key`/`value` with the 0-based message index — the one piece of per-message variation a burst template gets. */
function applySequence(template: ProduceMessage, seq: number): ProduceMessage {
  const substitute = (text: string): string => text.replaceAll('{{seq}}', String(seq));
  return {
    ...template,
    ...(template.key !== undefined ? { key: substitute(template.key) } : {}),
    value: template.value === null ? null : substitute(template.value),
  };
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        reject(new Error('burst cancelled'));
      },
      { once: true },
    );
  });
}

const PROGRESS_EVENT = 'progress';
/** Progress is only emitted every this-many messages (plus always on the last one) — emitting per message would flood a fast, high-rate burst with more updates than any UI can usefully render. */
const PROGRESS_EVERY = 20;

/** One running (or finished) burst job. Created and driven by {@link BurstJobManager}; routes only ever read it through {@link snapshot} and {@link onProgress}. */
export class BurstJob {
  readonly id = randomUUID();
  readonly total: number;
  private readonly emitter = new EventEmitter();
  private readonly controller = new AbortController();
  private sent = 0;
  private status: BurstJobStatus = 'running';
  private error: string | undefined;

  constructor(total: number) {
    this.total = total;
  }

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  snapshot(): BurstProgress {
    return {
      jobId: this.id,
      sent: this.sent,
      total: this.total,
      status: this.status,
      ...(this.error !== undefined ? { error: this.error } : {}),
    };
  }

  /** Subscribes to progress updates; returns an unsubscribe function. */
  onProgress(listener: (progress: BurstProgress) => void): () => void {
    this.emitter.on(PROGRESS_EVENT, listener);
    return () => this.emitter.off(PROGRESS_EVENT, listener);
  }

  recordSent(sent: number): void {
    this.sent = sent;
    this.emitter.emit(PROGRESS_EVENT, this.snapshot());
  }

  finish(status: 'completed' | 'cancelled' | 'failed', error?: string): void {
    this.status = status;
    this.error = error;
    this.emitter.emit(PROGRESS_EVENT, this.snapshot());
  }

  cancel(): void {
    this.controller.abort();
  }
}

async function runBurstJob(
  job: BurstJob,
  producer: PooledProducer,
  request: Pick<BurstRequest, 'topic' | 'template' | 'count' | 'ratePerSecond'>,
): Promise<void> {
  const delayMs = request.ratePerSecond !== undefined ? 1000 / request.ratePerSecond : 0;
  try {
    for (let index = 0; index < request.count; index += 1) {
      if (job.signal.aborted) break;

      await sendMessages(producer, {
        topic: request.topic,
        messages: [applySequence(request.template, index)],
        signal: job.signal,
      });

      const sent = index + 1;
      if (sent % PROGRESS_EVERY === 0 || sent === request.count) job.recordSent(sent);

      if (delayMs > 0 && sent < request.count) await sleep(delayMs, job.signal);
    }

    job.finish(job.signal.aborted ? 'cancelled' : 'completed');
  } catch (error) {
    if (job.signal.aborted) {
      job.finish('cancelled');
    } else {
      job.finish('failed', error instanceof Error ? error.message : String(error));
    }
  }
}

/** How long a finished job's final state stays queryable before it is forgotten — long enough for a client's SSE reconnect to still see it. */
const JOB_RETENTION_MS = 60_000;

export interface BurstSentInfo {
  readonly topic: string;
  /** Records sent since the previous notification, not the running total. */
  readonly count: number;
  /** `count` times the template's own byte length — `{{seq}}` substitution barely moves it, so this is a close estimate, not a per-record measurement. */
  readonly bytes: number;
}

export type BurstSentListener = (info: BurstSentInfo) => void;

function templateByteLength(template: Pick<ProduceMessage, 'key' | 'value'>): number {
  return (template.key?.length ?? 0) + (template.value?.length ?? 0);
}

/** Tracks in-flight and recently-finished burst jobs. One instance for the whole server process. */
export class BurstJobManager {
  private readonly jobs = new Map<string, BurstJob>();
  private readonly onSent: BurstSentListener | undefined;

  constructor(onSent?: BurstSentListener) {
    this.onSent = onSent;
  }

  start(producer: PooledProducer, request: BurstRequest): BurstJob {
    const job = new BurstJob(request.count);
    this.jobs.set(job.id, job);
    const bytesPerRecord = templateByteLength(request.template);

    let lastSent = 0;
    const unsubscribe = job.onProgress((progress) => {
      const delta = progress.sent - lastSent;
      if (delta > 0) {
        lastSent = progress.sent;
        this.onSent?.({ topic: request.topic, count: delta, bytes: delta * bytesPerRecord });
      }

      if (progress.status === 'running') return;
      unsubscribe();
      setTimeout(() => this.jobs.delete(job.id), JOB_RETENTION_MS).unref();
    });

    void runBurstJob(job, producer, request);
    return job;
  }

  get(jobId: string): BurstJob | undefined {
    return this.jobs.get(jobId);
  }

  /** Returns `false` when the job is unknown (already forgotten, or never existed). */
  cancel(jobId: string): boolean {
    const job = this.jobs.get(jobId);
    if (job === undefined) return false;
    job.cancel();
    return true;
  }

  cancelAll(): void {
    for (const job of this.jobs.values()) job.cancel();
  }
}
