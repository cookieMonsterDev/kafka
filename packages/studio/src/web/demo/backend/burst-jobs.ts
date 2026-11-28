import {
  applySequence,
  type BurstJobStatus,
  type BurstProgress,
  type BurstRequest,
} from '../../../shared/contracts/produce';
import type { DemoCluster } from './demo-cluster';

const TICK_MS = 100;

interface BurstJob {
  readonly id: string;
  readonly total: number;
  sent: number;
  status: BurstJobStatus;
  readonly listeners: Set<(progress: BurstProgress) => void>;
  timer: ReturnType<typeof setInterval> | undefined;
}

/** Rate-limited burst sends for the demo, reporting progress in batches like the real job manager. */
export class DemoBurstJobs {
  private readonly jobs = new Map<string, BurstJob>();
  private nextId = 1;
  private readonly cluster: DemoCluster;

  constructor(cluster: DemoCluster) {
    this.cluster = cluster;
  }

  start(request: BurstRequest): string {
    const job: BurstJob = {
      id: `burst-${String(this.nextId)}`,
      total: request.count,
      sent: 0,
      status: 'running',
      listeners: new Set(),
      timer: undefined,
    };
    this.nextId++;
    const { template } = request;
    const perTick = Math.max(1, Math.round((request.ratePerSecond * TICK_MS) / 1000));
    job.timer = setInterval(() => {
      const batch = Math.min(perTick, job.total - job.sent);
      const messages = Array.from({ length: batch }, (_, index) => applySequence(template, job.sent + index));
      try {
        this.cluster.produce({ topic: request.topic, messages }, 'studio');
        job.sent += batch;
        if (job.sent >= job.total) this.finish(job, 'completed');
        else this.notify(job);
      } catch (error) {
        this.finish(job, 'failed', error instanceof Error ? error.message : 'burst failed');
      }
    }, TICK_MS);
    this.jobs.set(job.id, job);
    return job.id;
  }

  cancel(jobId: string): boolean {
    const job = this.jobs.get(jobId);
    if (job === undefined) return false;
    if (job.status === 'running') this.finish(job, 'cancelled');
    return true;
  }

  /** Sends the current snapshot, then every update until the job finishes. `null` for an unknown job. */
  watch(jobId: string, listener: (progress: BurstProgress) => void, end: () => void): (() => void) | null {
    const job = this.jobs.get(jobId);
    if (job === undefined) return null;
    listener(this.snapshot(job));
    if (job.status !== 'running') {
      end();
      return () => {};
    }
    const forward = (progress: BurstProgress): void => {
      listener(progress);
      if (progress.status !== 'running') end();
    };
    job.listeners.add(forward);
    return () => job.listeners.delete(forward);
  }

  private snapshot(job: BurstJob, error?: string): BurstProgress {
    return {
      jobId: job.id,
      sent: job.sent,
      total: job.total,
      status: job.status,
      ...(error === undefined ? {} : { error }),
    };
  }

  private notify(job: BurstJob, error?: string): void {
    const progress = this.snapshot(job, error);
    for (const listener of [...job.listeners]) listener(progress);
  }

  private finish(job: BurstJob, status: Exclude<BurstJobStatus, 'running'>, error?: string): void {
    clearInterval(job.timer);
    job.status = status;
    this.notify(job, error);
    job.listeners.clear();
  }
}
