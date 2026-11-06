import type { Admin } from '@cookiemonsterdev/kafka-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TerminalSession } from './session';

const sessions: TerminalSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.close();
});
function session(readOnly = false, overrides: Partial<Admin> = {}) {
  let output = '';
  const admin = {
    connect: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
    listTopics: vi.fn(async () => ['orders']),
    deleteTopics: vi.fn(async () => {}),
    ...overrides,
  } as unknown as Admin;
  const shell = new TerminalSession({
    profile: null,
    readOnly,
    config: { path: null, fileConfig: null, profile: null, cli: {}, transformFallbackUsed: false },
    createAdmin: () => admin,
    listResources: async () => ({ topics: ['orders'], groups: ['payments worker'] }),
    onClose: () => {},
  });
  sessions.push(shell);
  shell.subscribe((data) => {
    output += data;
  });
  return { shell, admin, output: () => output };
}

describe('Kafka shell sessions', () => {
  it('runs CLI commands with native editing and history', async () => {
    const test = session();
    test.shell.input('topic lisx\x7ft\r');
    await vi.waitFor(() => expect(test.admin.listTopics).toHaveBeenCalledOnce());
    await vi.waitFor(() => expect(test.output()).toContain('orders'));
    test.shell.input('\x1b[A\r');
    await vi.waitFor(() => expect(test.admin.listTopics).toHaveBeenCalledTimes(2));
  });

  it('completes command paths through readline', async () => {
    const test = session();
    test.shell.input('top\t');
    await vi.waitFor(() => expect(test.output()).toContain('topic'));
    test.shell.input(' list\r');
    await vi.waitFor(() => expect(test.admin.listTopics).toHaveBeenCalledOnce());
  });

  it('keeps destructive confirmation interactive', async () => {
    const test = session();
    test.shell.input('topic delete orders\r');
    await vi.waitFor(() => expect(test.output()).toContain('[y/N]'));
    expect(test.admin.deleteTopics).not.toHaveBeenCalled();
    test.shell.input('y\r');
    await vi.waitFor(() => expect(test.admin.deleteTopics).toHaveBeenCalledOnce());
  });

  it('enforces read-only against the actual broker method', async () => {
    const test = session(true);
    test.shell.input('topic delete orders --yes\r');
    await vi.waitFor(() => expect(test.output()).toContain('read-only'));
    expect(test.admin.deleteTopics).not.toHaveBeenCalled();
  });

  it('interrupts an in-flight request and accepts another command', async () => {
    const test = session(false, { listTopics: vi.fn(() => new Promise<string[]>(() => {})) });
    test.shell.input('topic list\r');
    await vi.waitFor(() => expect(test.admin.listTopics).toHaveBeenCalledOnce());
    test.shell.input('\x03');
    await vi.waitFor(() => expect(test.output()).toContain('^C'));
    await vi.waitFor(() => expect(test.admin.disconnect).toHaveBeenCalled());
    test.shell.input('help\r');
    await vi.waitFor(() => expect(test.output()).toContain('Usage:'));
  });

  it('owns connecting clients when a request is cancelled', async () => {
    let connected: (() => void) | undefined;
    const test = session(false, {
      connect: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            connected = resolve;
          }),
      ),
    });
    test.shell.input('topic list\r');
    await vi.waitFor(() => expect(test.admin.connect).toHaveBeenCalledOnce());
    test.shell.input('\x03');
    await vi.waitFor(() => expect(test.admin.disconnect).toHaveBeenCalled());
    connected?.();
    await vi.waitFor(() => expect(test.admin.disconnect).toHaveBeenCalledTimes(2));
    expect(test.admin.listTopics).not.toHaveBeenCalled();
  });

  it('replays the terminal after reconnecting and closes on exit', async () => {
    const test = session();
    let replay = '';
    const unsubscribe = test.shell.subscribe((data) => {
      replay += data;
    });
    await vi.waitFor(() => expect(replay).toContain('kafka@direct'));
    unsubscribe();
    test.shell.resize(25);
    test.shell.input('exit\r');
    await vi.waitFor(() => expect(test.shell.isClosed).toBe(true));
  });
});
