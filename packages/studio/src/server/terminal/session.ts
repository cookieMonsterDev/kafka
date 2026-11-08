import { randomUUID } from 'node:crypto';
import type { CompleterResult } from 'node:readline';
import { Worker } from 'node:worker_threads';
import { PassThrough } from 'node:stream';
import {
  completeEmbeddedCli,
  embeddedCommandCatalog,
  runEmbeddedCli,
  type EmbeddedCommandSpec,
  type EmbeddedCliRuntime,
  type ResolvedCliConfig,
} from '@cookiemonsterdev/kafka-cli/embedded';
import type { Admin } from '@cookiemonsterdev/kafka-core';
import { terminalArgv, tokenizeTerminalInput } from '../../shared/terminal-input';

const MAX_REPLAY_BYTES = 512 * 1024;
const IDLE_TIMEOUT_MS = 5 * 60_000;

export interface TerminalSessionOptions {
  readonly profile: string | null;
  readonly readOnly: boolean;
  readonly config: ResolvedCliConfig;
  readonly createAdmin: () => Admin;
  readonly listResources: () => Promise<{ topics: readonly string[]; groups: readonly string[] }>;
  readonly commands?: readonly EmbeddedCommandSpec[];
  readonly onClose: () => void;
}

/** A Kafka-only shell: Node readline supplies real terminal editing; CLI supplies command behavior. */
export class TerminalSession {
  readonly id = randomUUID();
  readonly profile: string | null;
  private readonly commandInput = new PassThrough();
  private readonly editor: Worker;
  private columns = 80;
  private readonly options: TerminalSessionOptions;
  private readonly listeners = new Set<(data: string) => void>();
  private readonly history: string[] = [];
  private replayBytes = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private controller: AbortController | undefined;
  private closed = false;

  constructor(options: TerminalSessionOptions) {
    this.options = options;
    this.profile = options.profile;
    // Node readline consults TERM internally. Isolate its xterm environment so an IDE/service
    // launched with TERM=dumb still gets a real editor without mutating Studio's process.env.
    const workerFile = import.meta.url.endsWith('.ts') ? './readline-worker.ts' : './readline-worker.js';
    this.editor = new Worker(new URL(workerFile, import.meta.url), { env: { TERM: 'xterm-256color' } });
    this.editor.on('message', (message: { type: string; data?: string; line?: string; id?: number }) => {
      if (message.type === 'output') this.publish(message.data ?? '');
      else if (message.type === 'line') {
        if (this.controller !== undefined) {
          if (this.commandInput.listenerCount('data') > 0) this.commandInput.emit('data', `${message.line ?? ''}\n`);
        } else void this.run(message.line ?? '');
      } else if (message.type === 'interrupt') {
        if (this.controller !== undefined) {
          this.controller.abort('SIGINT');
          this.commandInput.emit('end');
        } else this.prompt();
      } else if (message.type === 'complete') {
        void this.complete(message.line ?? '').then(
          (result) => {
            if (!this.closed) this.editor.postMessage({ type: 'complete', id: message.id, result });
          },
          () => {
            if (!this.closed)
              this.editor.postMessage({ type: 'complete', id: message.id, result: [[], message.line ?? ''] });
          },
        );
      } else if (message.type === 'close') this.close();
    });
    this.editor.on('error', (error) => {
      this.publish(`Line editor failed: ${error instanceof Error ? error.message : String(error)}\r\n`);
      this.close();
    });
    this.write(
      `Kafka Studio shell · ${options.readOnly ? 'read-only' : 'writable'}\nType help for Kafka commands; clear clears the screen; exit closes the session.\n\n`,
    );
    this.prompt();
    this.armIdleTimer();
  }

  get isClosed(): boolean {
    return this.closed;
  }

  private write(data: string): boolean {
    if (this.closed) return false;
    this.editor.postMessage({ type: 'output', data: data.replace(/\r?\n/g, '\r\n') });
    return true;
  }

  private prompt(): void {
    const profile = (this.profile ?? 'direct').replace(/[^a-zA-Z0-9._-]/g, '_');
    this.editor.postMessage({ type: 'prompt', data: `\x1b[32mkafka@${profile}\x1b[0m:~$ ` });
  }

  private armIdleTimer(): void {
    clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.close(), IDLE_TIMEOUT_MS);
    this.idleTimer.unref();
  }

  private publish(data: string): void {
    if (this.closed) return;
    if (Buffer.byteLength(data) > MAX_REPLAY_BYTES)
      data = Buffer.from(data).subarray(-MAX_REPLAY_BYTES).toString('utf8');
    this.history.push(data);
    this.replayBytes += Buffer.byteLength(data);
    while (this.replayBytes > MAX_REPLAY_BYTES && this.history.length > 1) {
      this.replayBytes -= Buffer.byteLength(this.history.shift() ?? '');
    }
    for (const listener of this.listeners) listener(data);
  }

  subscribe(listener: (data: string) => void): () => void {
    clearTimeout(this.idleTimer);
    listener(`\x1bc${this.history.join('')}`);
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
      if (!this.closed && this.listeners.size === 0) this.armIdleTimer();
    };
  }

  input(data: string): void {
    if (!this.closed) this.editor.postMessage({ type: 'input', data });
  }

  resize(columns: number): void {
    this.columns = columns;
    this.editor.postMessage({ type: 'resize', columns });
  }

  private async complete(line: string): Promise<CompleterResult> {
    const tokens = tokenizeTerminalInput(line, false);
    const last = tokens.at(-1);
    const editing = last !== undefined && last.end === line.length;
    const current = editing ? (last?.value ?? '') : '';
    const raw = editing ? line.slice(last?.start ?? line.length) : '';
    const words = tokens.map((token) => token.value);
    if (!editing) words.push('');
    if (words[0] === 'kafka') words.shift();
    const completions = new Set(completeEmbeddedCli(words, this.options.commands));
    if (words.length <= 1)
      for (const word of ['help', 'clear', 'exit']) if (word.startsWith(current)) completions.add(word);
    const prior = words.slice(0, -1);
    const command = [
      ...embeddedCommandCatalog,
      ...(this.options.commands ?? []).map((command) => ({ ...command, positionals: command.positionals ?? [] })),
    ].find((item) => prior.slice(0, item.path.length).join(' ') === item.path.join(' '));
    if (command !== undefined) {
      const previous = prior.at(-1) ?? '';
      const positional = command.positionals[0]?.name.toLowerCase() ?? '';
      const topics =
        previous === '--topic' || previous === '--topics' || (positional.includes('topic') && !current.startsWith('-'));
      const groups = positional.includes('group') && !current.startsWith('-');
      if (topics || groups) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const empty = { topics: [], groups: [] };
        const resources = await Promise.race([
          this.options.listResources().catch(() => empty),
          new Promise<typeof empty>((resolve) => {
            timer = setTimeout(() => resolve(empty), 750);
          }),
        ]).finally(() => clearTimeout(timer));
        for (const name of topics ? resources.topics : resources.groups) {
          if (name.startsWith(current)) completions.add(/^[a-zA-Z0-9._-]+$/.test(name) ? name : JSON.stringify(name));
        }
      }
    }
    return [[...completions].sort(), raw];
  }

  private async run(line: string): Promise<void> {
    if (line.trim() === '') {
      this.prompt();
      return;
    }
    const controller = new AbortController();
    this.controller = controller;
    const admins = new Set<Admin>();
    this.editor.postMessage({ type: 'set-prompt', data: '' });
    try {
      const argv = terminalArgv(line);
      if (argv.length === 1 && argv[0] === 'clear') {
        this.write('\x1b[2J\x1b[3J\x1b[H');
        return;
      }
      if (argv.length === 1 && argv[0] === 'exit') {
        this.close();
        return;
      }
      const write = (chunk: string): boolean => {
        if (this.closed || controller.signal.aborted) return false;
        if (chunk.endsWith('[y/N] ')) {
          this.editor.postMessage({ type: 'prompt', data: chunk, history: false });
          return true;
        }
        return this.write(chunk);
      };
      const runtime: EmbeddedCliRuntime = {
        argv,
        env: {},
        cwd: '/',
        stdout: { write, isTTY: true, columns: this.columns },
        stderr: { write },
        stdin: Object.assign(this.commandInput, { isTTY: true }),
        isTty: true,
        columns: this.columns,
        now: () => new Date(),
        exit: () => {
          throw new Error('Embedded commands cannot exit Studio.');
        },
        signal: controller.signal,
        loadConfig: async () => this.options.config,
        openAdmin: async () => {
          const admin = this.options.createAdmin();
          admins.add(admin);
          // Own the client before connecting. An aborted dispatch can return while connect is
          // still unwinding; disconnect again if that underlying connection eventually opens.
          const connecting = admin.connect();
          void connecting
            .then(
              () => {
                if (controller.signal.aborted) return admin.disconnect();
              },
              () => {},
            )
            .catch(() => {});
          await connecting;
          controller.signal.throwIfAborted();
          return admin;
        },
      };
      let onAbort = () => {};
      const interrupted = new Promise<number>((resolve) => {
        onAbort = () => resolve(130);
        if (controller.signal.aborted) onAbort();
        else controller.signal.addEventListener('abort', onAbort, { once: true });
      });
      const code = await Promise.race([
        runEmbeddedCli(runtime, { readOnly: this.options.readOnly, additionalCommands: this.options.commands }),
        interrupted,
      ]).finally(() => controller.signal.removeEventListener('abort', onAbort));
      if (code !== 0) this.write(`\x1b[31mexit ${String(code)}\x1b[0m\n`);
    } catch (error) {
      this.write(`\x1b[31m${error instanceof Error ? error.message : String(error)}\x1b[0m\n`);
    } finally {
      await Promise.allSettled([...admins].map((admin) => admin.disconnect()));
      this.controller = undefined;
      if (!this.closed) this.prompt();
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.idleTimer);
    this.controller?.abort('SIGINT');
    this.commandInput.emit('end');
    void this.editor.terminate();
    this.commandInput.destroy();

    for (const listener of this.listeners) listener('\r\nSession closed.\r\n');
    this.listeners.clear();
    this.options.onClose();
  }
}
