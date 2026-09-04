import { describe, expect, it, vi } from 'vitest';
import { createRuntime, RuntimeExitCalledError, type RuntimeProcessLike } from './runtime';

function createFakeProcess(overrides: Partial<RuntimeProcessLike> = {}): RuntimeProcessLike & {
  emit(event: 'SIGINT' | 'SIGTERM'): void;
} {
  const listeners = new Map<string, (() => void)[]>();
  return {
    argv: ['/usr/bin/node', '/path/to/bin.js', '--port', '5757'],
    env: { FOO: 'bar' },
    platform: 'linux',
    stdout: { write: vi.fn(() => true) },
    stderr: { write: vi.fn(() => true) },
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

    expect(runtime.argv).toEqual(['--port', '5757']);
    expect(runtime.env).toEqual({ FOO: 'bar' });
    expect(runtime.platform).toBe('linux');
    expect(runtime.stdout).toBe(proc.stdout);
    expect(runtime.stderr).toBe(proc.stderr);
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
});
