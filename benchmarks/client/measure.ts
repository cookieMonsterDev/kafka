import { readFileSync } from 'node:fs';
import { type ELDHistogram, monitorEventLoopDelay, PerformanceObserver, performance } from 'node:perf_hooks';
import type { LatencySummary, RunMetrics } from './spec.ts';

const bytesPerMegabyte = 1_000_000;
const eventLoopResolutionMs = 10;
const cgroupCpuStatPath = '/sys/fs/cgroup/cpu.stat';
const cgroupMemoryPeakPath = '/sys/fs/cgroup/memory.peak';

/**
 * Fixed-capacity latency sample buffer. Storage is allocated up front so recording inside the
 * measured window never allocates; samples beyond `capacity` are counted but not stored.
 */
export class LatencyRecorder {
  readonly #samplesMs: Float64Array;
  #recorded = 0;
  #dropped = 0;

  constructor(capacity: number) {
    this.#samplesMs = new Float64Array(Math.max(1, capacity));
  }

  record(durationMs: number): void {
    if (this.#recorded === this.#samplesMs.length) {
      this.#dropped++;
      return;
    }
    this.#samplesMs[this.#recorded] = durationMs;
    this.#recorded++;
  }

  get recorded(): number {
    return this.#recorded;
  }

  get dropped(): number {
    return this.#dropped;
  }

  /** Nearest-rank percentiles over the stored samples; null when nothing was recorded. */
  summary(): LatencySummary | null {
    if (this.#recorded === 0) {
      return null;
    }
    const sorted = this.#samplesMs.slice(0, this.#recorded).sort();
    return {
      samples: this.#recorded,
      p50Ms: percentile(sorted, 50),
      p95Ms: percentile(sorted, 95),
      p99Ms: percentile(sorted, 99),
      maxMs: sorted[sorted.length - 1] ?? 0,
    };
  }
}

function percentile(sorted: Float64Array, percent: number): number {
  const rank = Math.ceil((percent / 100) * sorted.length) - 1;
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank))] ?? 0;
}

interface GcCounter {
  stop(): { count: number; totalMs: number };
}

/** `gc` entries exist on Node; Bun accepts the observer but never emits them, so it reports null. */
function startGcCounter(): GcCounter | null {
  if (!PerformanceObserver.supportedEntryTypes.includes('gc')) {
    return null;
  }
  let count = 0;
  let totalMs = 0;
  const addEntries = (entries: readonly PerformanceEntry[]): void => {
    for (const entry of entries) {
      count++;
      totalMs += entry.duration;
    }
  };
  const observer = new PerformanceObserver((list) => addEntries(list.getEntries()));
  try {
    observer.observe({ type: 'gc', buffered: false });
  } catch {
    observer.disconnect();
    return null;
  }
  return {
    stop() {
      addEntries(observer.takeRecords());
      observer.disconnect();
      return { count, totalMs };
    },
  };
}

// Node records the full interval between timer ticks (resolution plus delay), Bun only the delay
// past the resolution (checked on Node 26.13 and Bun 1.4.4), so Node's figures drop the resolution.
const eventLoopBaselineMs = process.versions.bun === undefined ? eventLoopResolutionMs : 0;

function startEventLoopMonitor(): ELDHistogram | null {
  if (typeof monitorEventLoopDelay !== 'function') {
    return null;
  }
  try {
    const histogram = monitorEventLoopDelay({ resolution: eventLoopResolutionMs });
    histogram.enable();
    return histogram;
  } catch {
    return null;
  }
}

function readCgroupCpuMicros(): number | null {
  try {
    const match = /^usage_usec (\d+)$/m.exec(readFileSync(cgroupCpuStatPath, 'utf8'));
    return match?.[1] === undefined ? null : Number(match[1]);
  } catch {
    return null;
  }
}

function readCgroupPeakMemoryBytes(): number | null {
  try {
    const value = Number(readFileSync(cgroupMemoryPeakPath, 'utf8').trim());
    return Number.isFinite(value) ? value : null;
  } catch {
    return null;
  }
}

export interface WindowResult {
  /** Records in the measured window. */
  records: number;
  /**
   * Value bytes in the measured window. Keys and headers are excluded so produce and consume
   * figures describe the same payload.
   */
  bytes: number;
  /** Summarized after the clocks stop, so sorting the samples stays out of the window. */
  latency: LatencyRecorder | null;
  startup?: RunMetrics['startup'];
}

export interface MeasurementWindow {
  readonly startedAtMs: number;
  /** Stops every collector and builds the metrics; call once, right after the window ends. */
  finish(result: WindowResult): RunMetrics;
}

export interface MeasurementOptions {
  /**
   * Take time and CPU baselines from process start (`performance.now()` 0 and zero CPU) instead of
   * now. Used by the startup scenario. GC and event-loop collectors still start now.
   */
  fromProcessStart?: boolean;
}

/**
 * Starts collecting metrics for one measured window. Throughput uses megabytes of 10^6 bytes.
 * CPU comes from `process.cpuUsage()` deltas and, inside a container, the cgroup v2 `cpu.stat`
 * delta; `memory.peak` is the container's lifetime peak. Event-loop delay is the p99 delay past a
 * 10 ms sampling interval, null for windows shorter than one interval. Unreadable cgroup files (for
 * example on a macOS host) and unsupported runtime collectors yield null.
 */
export function startMeasurement(options: MeasurementOptions = {}): MeasurementWindow {
  const fromProcessStart = options.fromProcessStart === true;
  const gcCounter = startGcCounter();
  const eventLoopMonitor = startEventLoopMonitor();
  const cgroupCpuStartMicros = fromProcessStart ? 0 : readCgroupCpuMicros();
  const cpuStart = fromProcessStart ? { user: 0, system: 0 } : process.cpuUsage();
  const startedAtMs = fromProcessStart ? 0 : performance.now();

  return {
    startedAtMs,
    finish(result) {
      const elapsedMs = performance.now() - startedAtMs;
      const cpu = process.cpuUsage(cpuStart);
      const cgroupCpuEndMicros = readCgroupCpuMicros();
      const gc = gcCounter?.stop() ?? null;
      eventLoopMonitor?.disable();
      const memory = process.memoryUsage();

      const elapsedSeconds = elapsedMs / 1000;
      const userSeconds = cpu.user / 1e6;
      const systemSeconds = cpu.system / 1e6;
      const cpuSeconds = userSeconds + systemSeconds;
      const cgroupCpuSeconds =
        cgroupCpuStartMicros === null || cgroupCpuEndMicros === null
          ? null
          : (cgroupCpuEndMicros - cgroupCpuStartMicros) / 1e6;
      const eventLoopDelayP99Ms =
        eventLoopMonitor === null || eventLoopMonitor.count === 0
          ? null
          : Math.max(0, eventLoopMonitor.percentile(99) / 1e6 - eventLoopBaselineMs);

      return {
        records: result.records,
        bytes: result.bytes,
        elapsedMs,
        recordsPerSecond: elapsedSeconds > 0 ? result.records / elapsedSeconds : 0,
        megabytesPerSecond: elapsedSeconds > 0 ? result.bytes / bytesPerMegabyte / elapsedSeconds : 0,
        latency: result.latency?.summary() ?? null,
        cpu: { userSeconds, systemSeconds },
        cgroupCpuSeconds,
        recordsPerCpuSecond: cpuSeconds > 0 ? result.records / cpuSeconds : 0,
        peakMemoryBytes: readCgroupPeakMemoryBytes(),
        rssBytes: memory.rss,
        heapUsedBytes: memory.heapUsed,
        gc,
        eventLoopDelayP99Ms,
        startup: result.startup ?? null,
      };
    },
  };
}
