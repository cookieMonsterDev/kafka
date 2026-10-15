import { describe, expect, it, vi } from 'vitest';
import { createRuntime, RuntimeExitCalledError, type RuntimeProcessLike } from './runtime';
import { EMPTY_RESOLVED_CLI_CONFIG } from './testing/create-command-context';

const { openAdminMock } = vi.hoisted(() => ({ openAdminMock: vi.fn(async () => ({})) }));
vi.mock('./admin/open', () => ({ openAdmin: openAdminMock }));

function createFakeProcess(overrides: Partial<RuntimeProcessLike> = {}): RuntimeProcessLike & {
  emit(event: 'SIGINT' | 'SIGTERM'): void;
} {
  const listeners = new Map<string, (() => void)[]>();
  return {
    argv: ['/usr/bin/node', '/path/to/bin.js', 'topic', 'list'],
    env: { FOO: 'bar' },
    cwd: () => '/work',
    stdout: { write: vi.fn(() => true), isTTY: false, columns: 80 },
    stderr: { write: vi.fn(() => true), isTTY: false, columns: 80 },
    stdin: { isTTY: false, setEncoding: vi.fn(), on: vi.fn(), removeListener: vi.fn(), pause: vi.fn() },
    exit: vi.fn() as unknown as RuntimeProcessLike['exit'],
    on(event, listener) {
      const list = listeners.get(event) ?? [];
      list.push(listener);
      listeners.set(event, list);
      return this;
    },
    emit(event) {
      for (const listener of listeners.get(event) ?? []) listener();
    },
    ...overrides,
  };
}

describe('createRuntime', () => {
  it('builds a runtime from an injected process-like object', () => {
    const proc = createFakeProcess();
    const runtime = createRuntime(proc);

    expect(runtime.argv).toEqual(['topic', 'list']);
    expect(runtime.env).toEqual({ FOO: 'bar' });
    expect(runtime.cwd).toBe('/work');
    expect(runtime.stdout).toBe(proc.stdout);
    expect(runtime.stderr).toBe(proc.stderr);
    expect(runtime.stdin).toBe(proc.stdin);
    expect(runtime.isTty).toBe(false);
    expect(runtime.columns).toBe(80);
  });

  it('defaults columns to 80 and isTty to false when the stream reports neither', () => {
    const proc = createFakeProcess({
      stdout: { write: vi.fn(() => true) },
    });
    const runtime = createRuntime(proc);

    expect(runtime.isTty).toBe(false);
    expect(runtime.columns).toBe(80);
  });

  it('exit() throws instead of ending the process', () => {
    const runtime = createRuntime(createFakeProcess());
    let thrown: unknown;

    expect(() => {
      try {
        runtime.exit(3);
      } catch (error) {
        thrown = error;
        throw error;
      }
    }).toThrow(RuntimeExitCalledError);
    expect(thrown).toBeInstanceOf(RuntimeExitCalledError);
    expect((thrown as RuntimeExitCalledError).code).toBe(3);
  });

  it('aborts the signal with reason SIGINT when the process receives SIGINT', () => {
    const proc = createFakeProcess();
    const runtime = createRuntime(proc);

    expect(runtime.signal.aborted).toBe(false);
    proc.emit('SIGINT');
    expect(runtime.signal.aborted).toBe(true);
    expect(runtime.signal.reason).toBe('SIGINT');
  });

  it('aborts the signal with reason SIGTERM when the process receives SIGTERM', () => {
    const proc = createFakeProcess();
    const runtime = createRuntime(proc);

    proc.emit('SIGTERM');
    expect(runtime.signal.aborted).toBe(true);
    expect(runtime.signal.reason).toBe('SIGTERM');
  });

  it('force-exits on a second signal so a hung shutdown can still be interrupted', () => {
    const exit = vi.fn();
    const proc = createFakeProcess({ exit: exit as unknown as RuntimeProcessLike['exit'] });
    createRuntime(proc);

    proc.emit('SIGINT');
    expect(exit).not.toHaveBeenCalled();
    proc.emit('SIGTERM');
    expect(exit).toHaveBeenCalledWith(143);
  });

  it('passes its signal to openAdmin so an in-flight connect aborts on SIGINT/SIGTERM', async () => {
    const runtime = createRuntime(createFakeProcess());
    await runtime.openAdmin({ env: {}, config: EMPTY_RESOLVED_CLI_CONFIG });

    expect(openAdminMock).toHaveBeenCalledWith(expect.objectContaining({ signal: runtime.signal }));
  });
});
