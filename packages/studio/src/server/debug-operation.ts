import type { IncomingMessage, ServerResponse } from 'node:http';
import { openSseStream, type SseStream } from './sse';

interface DebugOperation {
  readonly profile: string | null;
  readonly signal: AbortSignal;
  readonly stream: SseStream;
}

/** Owns admission and response lifetime for this server's streaming debugger operations. */
export class DebugOperations {
  private active = 0;
  private readonly getActiveProfile: () => string | null;

  constructor(getActiveProfile: () => string | null) {
    this.getActiveProfile = getActiveProfile;
  }

  async run(
    req: IncomingMessage,
    res: ServerResponse,
    execute: (operation: DebugOperation) => Promise<void>,
    options: { readonly failureMessage: string; readonly timeoutMs?: number },
  ): Promise<void> {
    if (this.active >= 4) throw new Error('Four debugger operations are already running; cancel one first');
    this.active++;
    const controller = new AbortController();
    let stream: SseStream | undefined;
    try {
      const profile = this.getActiveProfile();
      const signal =
        options.timeoutMs === undefined
          ? controller.signal
          : AbortSignal.any([controller.signal, AbortSignal.timeout(options.timeoutMs)]);
      // POST request closure follows body consumption; only response closure cancels the work.
      stream = openSseStream(req, res, () => controller.abort(), true);
      await execute({ profile, signal, stream });
    } catch (error) {
      if (stream === undefined) throw error;
      stream.send('operation-error', { message: error instanceof Error ? error.message : options.failureMessage });
    } finally {
      this.active--;
      controller.abort();
      stream?.close();
    }
  }
}
