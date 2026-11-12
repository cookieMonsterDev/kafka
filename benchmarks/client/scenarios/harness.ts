import type { AdapterOptions, BenchConsumer, BenchProducer, BenchRecord, ClientAdapter } from '../adapters/types.ts';
import type { ClientResult, RunMetrics, RunSpec, ScenarioDefinition } from '../spec.ts';

export type ObservedCounts = ClientResult['observed'];

/**
 * Producers and consumers a run opened. `main.ts` disconnects whatever is still open when the
 * scenario ends, fails or times out, so scenarios do not need their own cleanup paths.
 */
export class ClientTracker {
  readonly #open = new Set<BenchProducer | BenchConsumer>();

  async connectProducer(adapter: ClientAdapter, options: AdapterOptions): Promise<BenchProducer> {
    const producer = adapter.createProducer(options);
    this.#open.add(producer);
    await producer.connect();
    return producer;
  }

  async connectConsumer(adapter: ClientAdapter, options: AdapterOptions): Promise<BenchConsumer> {
    const consumer = adapter.createConsumer(options);
    this.#open.add(consumer);
    await consumer.connect();
    return consumer;
  }

  /** Disconnects every open client in parallel; returns the error messages instead of throwing. */
  async disconnectAll(): Promise<string[]> {
    const clients = [...this.#open];
    this.#open.clear();
    const outcomes = await Promise.allSettled(clients.map((client) => client.disconnect()));
    return outcomes.flatMap((outcome) =>
      outcome.status === 'rejected' ? [`disconnect failed: ${describeError(outcome.reason)}`] : [],
    );
  }
}

export interface ScenarioContext {
  spec: RunSpec;
  definition: ScenarioDefinition;
  adapter: ClientAdapter;
  adapterOptions: AdapterOptions;
  /** Seeded record pool; scenarios cycle through it and never build records in a measured window. */
  records: readonly BenchRecord[];
  clients: ClientTracker;
  /** Updated while the run progresses, so a timeout still reports how far it got. */
  observed: ObservedCounts;
}

export type ScenarioRunner = (context: ScenarioContext) => Promise<RunMetrics | null>;

/** The pooled record at `index`, cycling through the pool. */
export function pooledRecord(records: readonly BenchRecord[], index: number): BenchRecord {
  const record = records[index % records.length];
  if (record === undefined) {
    throw new Error('record pool is empty');
  }
  return record;
}

export function describeError(error: unknown): string {
  if (error instanceof Error) {
    const causeMessage = error.cause instanceof Error ? error.cause.message : String(error.cause);
    const isCauseNew = error.cause !== undefined && !error.message.includes(causeMessage);
    const cause = isCauseNew ? ` (cause: ${describeError(error.cause)})` : '';
    return `${error.name}: ${error.message}${cause}`;
  }
  return String(error);
}

/**
 * A promise plus its settle functions, for completion signalled from a library callback. A
 * rejection before anyone awaits it is not reported as unhandled; awaiting it still throws.
 */
export function createSignal<T>(): { promise: Promise<T>; resolve(value: T): void; reject(error: unknown): void } {
  let resolve: (value: T) => void = () => undefined;
  let reject: (error: unknown) => void = () => undefined;
  const promise = new Promise<T>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}
