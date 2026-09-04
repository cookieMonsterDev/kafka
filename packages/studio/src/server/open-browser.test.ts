import type { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { formatBanner, openBrowser } from './open-browser';

function fakeSpawn(behavior: 'success' | 'error') {
  const spawnFn = vi.fn(() => {
    const child = new EventEmitter() as EventEmitter & { unref: () => void };
    child.unref = vi.fn();
    queueMicrotask(() => {
      if (behavior === 'success') child.emit('spawn');
      else child.emit('error', new Error('ENOENT'));
    });
    return child;
  });
  return spawnFn as unknown as typeof spawn;
}

describe('openBrowser', () => {
  it('does nothing and returns false when BROWSER=none', async () => {
    const spawnFn = fakeSpawn('success');
    const opened = await openBrowser('http://localhost:1234', undefined, {
      env: { BROWSER: 'none' },
      platform: 'linux',
      spawn: spawnFn,
    });
    expect(opened).toBe(false);
    expect(spawnFn).not.toHaveBeenCalled();
  });

  it('an explicit override of "none" wins over BROWSER', async () => {
    const spawnFn = fakeSpawn('success');
    const opened = await openBrowser('http://localhost:1234', 'none', {
      env: { BROWSER: 'firefox' },
      platform: 'linux',
      spawn: spawnFn,
    });
    expect(opened).toBe(false);
    expect(spawnFn).not.toHaveBeenCalled();
  });

  it('uses xdg-open on linux by default', async () => {
    const spawnFn = fakeSpawn('success');
    await openBrowser('http://localhost:1234', undefined, { env: {}, platform: 'linux', spawn: spawnFn });
    expect(spawnFn).toHaveBeenCalledWith('xdg-open', ['http://localhost:1234'], expect.anything());
  });

  it('uses open on darwin by default', async () => {
    const spawnFn = fakeSpawn('success');
    await openBrowser('http://localhost:1234', undefined, { env: {}, platform: 'darwin', spawn: spawnFn });
    expect(spawnFn).toHaveBeenCalledWith('open', ['http://localhost:1234'], expect.anything());
  });

  it('uses the BROWSER env override as the command', async () => {
    const spawnFn = fakeSpawn('success');
    await openBrowser('http://localhost:1234', undefined, {
      env: { BROWSER: 'firefox' },
      platform: 'linux',
      spawn: spawnFn,
    });
    expect(spawnFn).toHaveBeenCalledWith('firefox', ['http://localhost:1234'], expect.anything());
  });

  it('an explicit override wins over BROWSER', async () => {
    const spawnFn = fakeSpawn('success');
    await openBrowser('http://localhost:1234', 'chrome', {
      env: { BROWSER: 'firefox' },
      platform: 'linux',
      spawn: spawnFn,
    });
    expect(spawnFn).toHaveBeenCalledWith('chrome', ['http://localhost:1234'], expect.anything());
  });

  it('never throws or rejects when spawning fails, and reports it did not open', async () => {
    const spawnFn = fakeSpawn('error');
    await expect(
      openBrowser('http://localhost:1234', undefined, { env: {}, platform: 'linux', spawn: spawnFn }),
    ).resolves.toBe(false);
  });
});

describe('formatBanner', () => {
  it('prints the actual bound address', () => {
    expect(formatBanner({ url: 'http://0.0.0.0:5757/', readOnly: false })).toContain('http://0.0.0.0:5757/');
  });

  it('adds a read-only notice when enabled', () => {
    const banner = formatBanner({ url: 'http://127.0.0.1:5757/', readOnly: true });
    expect(banner).toContain('read-only');
  });

  it('omits the read-only notice by default', () => {
    const banner = formatBanner({ url: 'http://127.0.0.1:5757/', readOnly: false });
    expect(banner).not.toContain('read-only');
  });
});
