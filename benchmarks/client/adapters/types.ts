import type { ClientId, Profile, ScenarioDefinition, WireSettings } from '../spec.ts';

/** One record as every adapter receives it: `Buffer` key, value and header values, no serializers. */
export interface BenchRecord {
  key: Buffer;
  value: Buffer;
  headers?: Readonly<Record<string, Buffer>>;
}

export interface AdapterOptions {
  brokers: string[];
  clientId: string;
  profile: Profile;
  /** Ignored by adapters under the `defaults` profile, except where a setting is required to run. */
  wire: WireSettings;
}

export interface BenchProducer {
  connect(): Promise<void>;
  /** Resolves once the broker acknowledged every record in `records` for the configured acks. */
  send(topic: string, records: readonly BenchRecord[]): Promise<void>;
  disconnect(): Promise<void>;
}

export interface ConsumeOptions {
  topic: string;
  groupId: string;
  /** `each-message` calls `onMessage` per record; `each-batch` calls `onBatch` per fetched batch. */
  mode: 'each-message' | 'each-batch';
  /**
   * Under `normalized`: commit explicitly every N records with auto-commit disabled; 0 disables
   * committing entirely, so commits never differ between clients unless a scenario asks for them.
   * Under `defaults`: 0 leaves the library's own auto-commit behaviour in place.
   */
  commitEvery: number;
  onMessage(value: Buffer | null): void;
  /** `bytes` is the sum of value lengths in the batch. */
  onBatch(count: number, bytes: number): void;
}

export interface BenchConsumer {
  connect(): Promise<void>;
  /** Subscribes from the earliest offset and starts delivering. Resolves once consumption started. */
  start(options: ConsumeOptions): Promise<void>;
  disconnect(): Promise<void>;
}

/**
 * Settings or behaviour a library cannot express for this scenario. Each entry is a short
 * human-readable footnote such as `lingerMs: no linger support, records are sent per send() call`.
 * Empty when nothing is missing.
 */
export type CapabilityGaps = (options: AdapterOptions, scenario: ScenarioDefinition) => string[];

export interface ClientAdapter {
  id: ClientId;
  packageName: string;
  /** Installed package version, read from its package.json at run time. */
  version(): string;
  capabilityGaps: CapabilityGaps;
  createProducer(options: AdapterOptions): BenchProducer;
  createConsumer(options: AdapterOptions): BenchConsumer;
}
