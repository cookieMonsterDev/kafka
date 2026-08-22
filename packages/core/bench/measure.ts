import { constants, PerformanceObserver, performance } from 'node:perf_hooks';

export interface BenchStats {
  name: string;
  iterations: number;
  messages: number;
  bytes: number;
  elapsedMs: number;
  msgsPerSec: number;
  mbPerSec: number;
  p50Ms: number;
  p99Ms: number;
  rssMb: number;
  heapUsedMb: number;
  gcCount: number;
  youngGcCount: number;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[rank] ?? 0;
}

function startGcCounter(): () => { gcCount: number; youngGcCount: number } {
  let gcCount = 0;
  let youngGcCount = 0;

  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      gcCount += 1;
      const kind = (entry as PerformanceEntry & { detail?: { kind?: number } }).detail?.kind;
      if (kind === constants.NODE_PERFORMANCE_GC_MINOR) {
        youngGcCount += 1;
      }
    }
  });

  try {
    observer.observe({ type: 'gc', buffered: false });
  } catch {
    observer.disconnect();
    return () => ({ gcCount: 0, youngGcCount: 0 });
  }

  return () => {
    observer.disconnect();
    return { gcCount, youngGcCount };
  };
}

export async function measure(options: {
  name: string;
  warmup?: number;
  iterations: number;
  messagesPerIter: number;
  bytesPerIter: number;
  run: () => Promise<void> | void;
}): Promise<BenchStats> {
  const warmup = options.warmup ?? Math.min(3, options.iterations);
  for (let i = 0; i < warmup; i++) {
    await options.run();
  }

  const stopGc = startGcCounter();
  const samples: number[] = [];
  const started = performance.now();

  for (let i = 0; i < options.iterations; i++) {
    const t0 = performance.now();
    await options.run();
    samples.push(performance.now() - t0);
  }

  const elapsedMs = performance.now() - started;
  samples.sort((a, b) => a - b);
  const { gcCount, youngGcCount } = stopGc();
  const memory = process.memoryUsage();
  const messages = options.messagesPerIter * options.iterations;
  const bytes = options.bytesPerIter * options.iterations;
  const elapsedSec = elapsedMs / 1000;

  return {
    name: options.name,
    iterations: options.iterations,
    messages,
    bytes,
    elapsedMs,
    msgsPerSec: elapsedSec > 0 ? messages / elapsedSec : 0,
    mbPerSec: elapsedSec > 0 ? bytes / elapsedSec / (1024 * 1024) : 0,
    p50Ms: percentile(samples, 50),
    p99Ms: percentile(samples, 99),
    rssMb: memory.rss / (1024 * 1024),
    heapUsedMb: memory.heapUsed / (1024 * 1024),
    gcCount,
    youngGcCount,
  };
}

export function formatStats(stats: BenchStats): string {
  const msgs = stats.msgsPerSec.toFixed(0);
  const mb = stats.mbPerSec.toFixed(2);
  return [
    stats.name,
    `  ${stats.iterations} iters, ${stats.messages} msgs, ${(stats.bytes / (1024 * 1024)).toFixed(2)} MiB total`,
    `  ${msgs} msgs/s, ${mb} MB/s, p50 ${stats.p50Ms.toFixed(2)} ms, p99 ${stats.p99Ms.toFixed(2)} ms`,
    `  RSS ${stats.rssMb.toFixed(1)} MB, heap ${stats.heapUsedMb.toFixed(1)} MB, GC ${stats.gcCount} (young ${stats.youngGcCount})`,
  ].join('\n');
}

export function printStats(stats: BenchStats): void {
  console.log(formatStats(stats));
}

export function liveBrokers(): string[] | null {
  const fromEnv = process.env.KAFKA_BROKERS;
  if (fromEnv && fromEnv.trim() !== '') {
    return fromEnv
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  }
  if (process.env.KAFKA_EXTERNAL === '1') {
    return ['localhost:9092'];
  }
  return null;
}
