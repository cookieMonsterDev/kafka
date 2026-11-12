import { createTopic, deleteTopic } from './broker.ts';
import { runClientContainer } from './container.ts';
import { errorMessage } from './process.ts';
import type { BrokerHandle, ClientContainerOptions, ClientContainerOutcome } from './types.ts';

interface AttemptTarget {
  broker: BrokerHandle;
  topic: string;
  partitions: number;
}

interface Attempt {
  runClient(options: ClientContainerOptions): Promise<ClientContainerOutcome>;
}

type AttemptResult<T> =
  { ok: true; value: T; cleanupErrors: string[] } | { ok: false; error: unknown; cleanupErrors: string[] };

interface AttemptDependencies {
  createTopic: typeof createTopic;
  deleteTopic: typeof deleteTopic;
  runClientContainer: typeof runClientContainer;
}

/** Owns attempt resources until setup, execution, and disposal have all settled, including on shutdown. */
export class BenchmarkAttempts {
  private readonly dependencies: AttemptDependencies;
  private readonly active = new Map<AbortController, Promise<void>>();
  private isStopping = false;

  constructor(dependencies: AttemptDependencies = { createTopic, deleteTopic, runClientContainer }) {
    this.dependencies = dependencies;
  }

  async run<T>(target: AttemptTarget, execute: (attempt: Attempt) => Promise<T>): Promise<AttemptResult<T>> {
    if (this.isStopping) throw new Error('Benchmark attempts are stopping');
    const controller = new AbortController();
    const finished = Promise.withResolvers<void>();
    this.active.set(controller, finished.promise);
    const cleanupErrors: string[] = [];
    try {
      try {
        // A failed create can still have created the topic; disposal must cover the whole acquisition.
        await this.dependencies.createTopic(target.broker, target.topic, target.partitions);
        controller.signal.throwIfAborted();
        const value = await execute({
          runClient: async (options) => {
            controller.signal.throwIfAborted();
            return this.dependencies.runClientContainer({
              ...options,
              signal: controller.signal,
              onCleanupError: (error) => cleanupErrors.push(`cannot dispose client container: ${errorMessage(error)}`),
            });
          },
        });
        controller.signal.throwIfAborted();
        return { ok: true, value, cleanupErrors };
      } catch (error) {
        return { ok: false, error, cleanupErrors };
      } finally {
        try {
          await this.dependencies.deleteTopic(target.broker, target.topic);
        } catch (error) {
          cleanupErrors.push(`cannot delete topic ${target.topic}: ${errorMessage(error)}`);
        }
      }
    } finally {
      this.active.delete(controller);
      finished.resolve();
    }
  }

  /** Stops admission and waits for in-flight setup and container cancellation before topic disposal completes. */
  async cancelAll(): Promise<void> {
    this.isStopping = true;
    const pending = [...this.active.entries()];
    for (const [controller] of pending) controller.abort(new Error('Benchmark attempt cancelled'));
    await Promise.all(pending.map(([, finished]) => finished));
  }
}
