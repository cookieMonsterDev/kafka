// Shared contract between the host-side runner and the in-container client.
// Type-only plus plain data: both sides import it, and the client must stay free of dependencies.

export const resultSchemaVersion = 1;

/** Prefix of the single stdout line that carries a run's result. */
export const resultLineMarker = '@@BENCH_RESULT@@ ';

/** Environment variable that carries the JSON-encoded `RunSpec` into the client container. */
export const specEnvVar = 'BENCH_SPEC';

export const clientIds = ['kafka-core', 'platformatic', 'kafkajs', 'confluent'] as const;
export type ClientId = (typeof clientIds)[number];

export const runtimeIds = ['node24', 'node26', 'bun'] as const;
export type RuntimeId = (typeof runtimeIds)[number];

/** npm package behind each client id. */
export const clientPackageNames: Readonly<Record<ClientId, string>> = {
  'kafka-core': '@cookiemonsterdev/kafka-core',
  platformatic: '@platformatic/kafka',
  kafkajs: 'kafkajs',
  confluent: '@confluentinc/kafka-javascript',
};

export const profiles = ['normalized', 'defaults'] as const;
export type Profile = (typeof profiles)[number];

export const tiers = ['smoke', 'standard', 'full'] as const;
export type Tier = (typeof tiers)[number];

export const payloadSizes = ['small', 'medium'] as const;
export type PayloadSize = (typeof payloadSizes)[number];

/**
 * Seed of the record pool. Fixed, unlike the per-session shuffle seed, so every session sends the
 * same bytes and compression ratios (P4, C3) stay comparable between sessions.
 */
export const payloadSeed = 0x6b61666b;

export const scenarioIds = ['P1', 'P2', 'P3', 'P4', 'P5', 'C1', 'C2', 'C3', 'C4', 'E1', 'L1'] as const;
export type ScenarioId = (typeof scenarioIds)[number];

export type ScenarioKind = 'produce' | 'consume' | 'e2e' | 'startup';

export const compressions = ['none', 'gzip'] as const;
export type Compression = (typeof compressions)[number];

/** Producer acks the scenarios use: leader only (1) or all in-sync replicas (-1). */
export const acksLevels = [-1, 1] as const;
export type Acks = (typeof acksLevels)[number];

/**
 * Wire-level settings shared by every client under the `normalized` profile. Adapters map these to
 * library options; under `defaults` they ignore everything except brokers and topic.
 */
export interface WireSettings {
  acks: Acks;
  lingerMs: number;
  batchBytes: number;
  compression: Compression;
  maxInFlight: number;
  idempotent: boolean;
  fetchMinBytes: number;
  fetchMaxBytes: number;
  fetchMaxWaitMs: number;
  partitionFetchMaxBytes: number;
}

export interface ScenarioDefinition {
  id: ScenarioId;
  name: string;
  kind: ScenarioKind;
  tiers: readonly Tier[];
  /** Profiles this scenario runs under. `defaults` is limited to the throughput scenarios. */
  profiles: readonly Profile[];
  wire: WireSettings;
  /** Records per `send()` call for produce scenarios. */
  recordsPerSend: number;
  /** Maximum concurrent in-flight `send()` calls; 1 means sequential `await`. */
  concurrency: number;
  /** Consume through the batch API instead of the per-record handler. */
  consumeMode: 'each-message' | 'each-batch';
  /** Commit offsets every N records (C4); 0 means no commits (see `ConsumeOptions.commitEvery`). */
  commitEvery: number;
  /** Compression used by the neutral seeder for consume scenarios. */
  seedCompression: Compression;
}

const baseWire: WireSettings = {
  acks: 1,
  lingerMs: 0,
  batchBytes: 16_384,
  compression: 'none',
  maxInFlight: 5,
  idempotent: false,
  fetchMinBytes: 1,
  fetchMaxBytes: 52_428_800,
  fetchMaxWaitMs: 100,
  partitionFetchMaxBytes: 1_048_576,
};

const throughputWire: WireSettings = { ...baseWire, lingerMs: 5 };

const scenarioBase = {
  profiles: ['normalized'],
  wire: baseWire,
  recordsPerSend: 1,
  concurrency: 1,
  consumeMode: 'each-message',
  commitEvery: 0,
  seedCompression: 'none',
} as const satisfies Partial<ScenarioDefinition>;

export const scenarios: Readonly<Record<ScenarioId, ScenarioDefinition>> = {
  P1: { ...scenarioBase, id: 'P1', name: 'produce-single', kind: 'produce', tiers: ['smoke', 'standard', 'full'] },
  P2: {
    ...scenarioBase,
    id: 'P2',
    name: 'produce-batch',
    kind: 'produce',
    tiers: ['standard', 'full'],
    recordsPerSend: 100,
  },
  P3: {
    ...scenarioBase,
    id: 'P3',
    name: 'produce-throughput',
    kind: 'produce',
    tiers: ['smoke', 'standard', 'full'],
    profiles: ['normalized', 'defaults'],
    wire: throughputWire,
    concurrency: 256,
  },
  P4: {
    ...scenarioBase,
    id: 'P4',
    name: 'produce-gzip',
    kind: 'produce',
    tiers: ['full'],
    wire: { ...throughputWire, compression: 'gzip' },
    concurrency: 256,
  },
  P5: {
    ...scenarioBase,
    id: 'P5',
    name: 'produce-idempotent',
    kind: 'produce',
    tiers: ['full'],
    wire: { ...throughputWire, acks: -1, idempotent: true },
    concurrency: 256,
  },
  C1: {
    ...scenarioBase,
    id: 'C1',
    name: 'consume-each-message',
    kind: 'consume',
    tiers: ['smoke', 'standard', 'full'],
    profiles: ['normalized', 'defaults'],
  },
  C2: {
    ...scenarioBase,
    id: 'C2',
    name: 'consume-each-batch',
    kind: 'consume',
    tiers: ['standard', 'full'],
    consumeMode: 'each-batch',
  },
  C3: {
    ...scenarioBase,
    id: 'C3',
    name: 'consume-gzip',
    kind: 'consume',
    tiers: ['full'],
    seedCompression: 'gzip',
  },
  C4: { ...scenarioBase, id: 'C4', name: 'consume-commit', kind: 'consume', tiers: ['full'], commitEvery: 1000 },
  E1: { ...scenarioBase, id: 'E1', name: 'e2e-latency', kind: 'e2e', tiers: ['standard', 'full'] },
  L1: { ...scenarioBase, id: 'L1', name: 'startup', kind: 'startup', tiers: ['standard', 'full'] },
};

/** Everything the client process needs for one run. Serialized as JSON into `BENCH_SPEC`. */
export interface RunSpec {
  runId: string;
  scenario: ScenarioId;
  client: ClientId;
  runtime: RuntimeId;
  profile: Profile;
  /** Free-form label that separates two series of the same client, e.g. the A/A noise test. */
  series: string;
  repetition: number;
  brokers: string[];
  topic: string;
  partitions: number;
  groupId: string;
  payload: PayloadSize;
  /** Record pool seed; the runner passes `payloadSeed`. */
  seed: number;
  /** Records in the measured window (produce and consume). */
  messageCount: number;
  /** Records sent or consumed before the measured window starts. */
  warmupCount: number;
  /** E1 only: open-loop send rate and measured duration. */
  rateMessagesPerSecond: number;
  durationMs: number;
  wire: WireSettings;
  /** Hard deadline for the whole run inside the client; the runner kills the container later. */
  timeoutMs: number;
}

export interface LatencySummary {
  samples: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
}

export interface RunMetrics {
  records: number;
  bytes: number;
  elapsedMs: number;
  recordsPerSecond: number;
  megabytesPerSecond: number;
  latency: LatencySummary | null;
  cpu: { userSeconds: number; systemSeconds: number };
  /** cgroup `cpu.stat` usage for the whole container, when readable. */
  cgroupCpuSeconds: number | null;
  recordsPerCpuSecond: number;
  /** cgroup `memory.peak`, when readable. */
  peakMemoryBytes: number | null;
  rssBytes: number;
  heapUsedBytes: number;
  gc: { count: number; totalMs: number } | null;
  eventLoopDelayP99Ms: number | null;
  /** L1 only: milliseconds from process start. */
  startup: { firstAckedProduceMs: number; firstConsumedRecordMs: number } | null;
}

export type RunStatus = 'ok' | 'failed' | 'unsupported';

/** What the client prints after `resultLineMarker`. The runner adds verification afterwards. */
export interface ClientResult {
  schemaVersion: typeof resultSchemaVersion;
  runId: string;
  status: RunStatus;
  error: string | null;
  runtime: { id: RuntimeId; name: 'node' | 'bun'; version: string };
  clientVersion: string | null;
  capabilityGaps: string[];
  /** Records and bytes the client itself counted; the runner compares them with the broker. */
  observed: { producedRecords: number; consumedRecords: number; consumedBytes: number };
  metrics: RunMetrics | null;
}

export interface Verification {
  passed: boolean;
  details: string;
}

/** A run as stored by the runner under `results/raw/<session>/runs/`. */
export interface RunRecord {
  spec: RunSpec;
  result: ClientResult;
  verification: Verification;
  image: { reference: string; digest: string | null };
  startedAt: string;
  wallMs: number;
}
