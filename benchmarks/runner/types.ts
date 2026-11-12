// Contracts between runner modules that are implemented in separate files.

import type { ClientId, ClientResult, PayloadSize, Profile, RuntimeId, ScenarioId, Tier } from '../client/spec.ts';

/** CPU and memory split derived from `docker info` (see README, "Resource budget"). */
export interface ResourceBudget {
  vmCpus: number;
  vmMemoryBytes: number;
  brokerCpuset: string;
  brokerMemoryBytes: number;
  brokerHeapBytes: number;
  brokerTmpfsBytes: number;
  clientCpuset: string;
  clientMemoryBytes: number;
}

export interface DoctorCheck {
  name: string;
  status: 'pass' | 'warn' | 'fail';
  details: string;
}

/** Written to `<session>/environment.json`. */
export interface EnvironmentReport {
  takenAt: string;
  host: { platform: string; arch: string; cpuModel: string; cpuCount: number; memoryBytes: number; osRelease: string };
  docker: { serverVersion: string; vmCpus: number; vmMemoryBytes: number; kernel: string };
  budget: ResourceBudget;
  checks: DoctorCheck[];
  forced: boolean;
  /** Names of checks that were `warn`, or `fail` downgraded by `--force`. Non-empty blocks publishing. */
  warnings: string[];
}

export interface BrokerHandle {
  containerName: string;
  network: string;
  /** Address the client containers use, e.g. `broker:9092`. */
  bootstrap: string;
  image: string;
  imageDigest: string | null;
}

export interface CoreSource {
  /** What the user asked for: `origin/develop`, another ref, or `WORKTREE`. */
  requestedRef: string;
  /** Full commit SHA; for `WORKTREE` the HEAD SHA, with `isDirty` telling whether it differs. */
  sha: string;
  commitDate: string;
  isWorktree: boolean;
  isDirty: boolean;
  isOnOriginDevelop: boolean;
}

export interface RuntimeImage {
  runtime: RuntimeId;
  /** Local tag passed to `docker run`. */
  reference: string;
  imageId: string;
  baseImage: string;
  baseDigest: string | null;
  runtimeVersion: string;
  /** Installed versions of each client package inside the image; null when not installed. */
  clientVersions: Record<ClientId, string | null>;
}

export interface SupportCell {
  client: ClientId;
  runtime: RuntimeId;
  supported: boolean;
  /** Empty when supported; otherwise the first meaningful line of the failure. */
  reason: string;
}

/** Written to `<session>/support-matrix.json`. */
export interface SupportMatrix {
  /** Only the runtimes that were probed. */
  imageIds: Partial<Record<RuntimeId, string>>;
  cells: SupportCell[];
}

export interface ClientContainerOptions {
  image: string;
  /** JSON-encoded into the `BENCH_SPEC` environment variable. */
  spec: unknown;
  network: string;
  cpuset: string;
  memoryBytes: number;
  timeoutMs: number;
  /** Arguments appended to the image entrypoint (`<runtime> client/main.ts`), e.g. `['probe']`. */
  args?: string[];
}

export interface ClientContainerOutcome {
  /** Parsed from the `resultLineMarker` line; null when no valid line was printed. */
  result: ClientResult | null;
  exitCode: number | null;
  timedOut: boolean;
  wallMs: number;
  stdoutTail: string;
  stderrTail: string;
}

export interface Footprint {
  client: ClientId;
  packageName: string;
  version: string | null;
  installBytes: number | null;
  dependencyCount: number | null;
  hasNativeAddon: boolean | null;
}

/** Written to `<session>/session.json` by `run`; the session directory is `results/raw/<sessionId>/`. */
export interface SessionManifest {
  /** `<UTC yyyymmddHHMMSS>-<first 12 chars of core SHA>`. */
  sessionId: string;
  tier: Tier;
  seed: number;
  startedAt: string;
  finishedAt: string | null;
  repetitions: number;
  clients: ClientId[];
  runtimes: RuntimeId[];
  scenarios: ScenarioId[];
  profiles: Profile[];
  payload: PayloadSize;
  /** `--count-scale` applied to the tier's message counts and E1 duration. */
  countScale: number;
  /** Series labels; normally one per client (the client id), plus `kafka-core#2` for the A/A test. */
  series: string[];
  core: CoreSource;
  images: RuntimeImage[];
  broker: { image: string; digest: string | null };
  plannedRuns: number;
}
