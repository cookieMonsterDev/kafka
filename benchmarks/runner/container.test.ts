import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { removeContainer, runClientContainer } from './container.ts';
import type { ClientContainerOptions } from './types.ts';

// A real child process exercises close/abort/timeout ordering without starting Docker.
const fakeDocker = `#!${process.execPath}
const fs = require('node:fs');
const path = require('node:path');
const directory = __dirname;
const event = (value) => fs.appendFileSync(path.join(directory, 'events'), value + '\\n');
const command = process.argv[2];
if (command === 'run') {
  event('run');
  fs.writeFileSync(path.join(directory, 'pid'), String(process.pid));
  if (process.argv.includes('finish')) process.exit(0);
  setInterval(() => {}, 1000);
} else if (command === 'kill') {
  event('kill');
  try { process.kill(Number(fs.readFileSync(path.join(directory, 'pid'), 'utf8')), 'SIGTERM'); } catch {}
} else if (command === 'rm') {
  if (fs.existsSync(path.join(directory, 'fail-removal'))) { process.stderr.write('permission denied'); process.exit(1); }
  if (fs.existsSync(path.join(directory, 'missing'))) { process.stderr.write('Error response from daemon: No such container: test'); process.exit(1); }
  setTimeout(() => { event('removed'); }, 50);
}
`;

describe('client container termination', { skip: process.platform === 'win32' }, () => {
  let directory: string;
  let previousPath: string | undefined;
  const options: ClientContainerOptions = {
    image: 'fake',
    spec: { runId: 'termination-test' },
    network: 'fake',
    cpuset: '0',
    memoryBytes: 1024,
    timeoutMs: 5000,
  };
  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'benchmark-docker-test-'));
    await writeFile(join(directory, 'docker'), fakeDocker);
    await chmod(join(directory, 'docker'), 0o755);
    previousPath = process.env.PATH;
    process.env.PATH = `${directory}:${previousPath ?? ''}`;
  });
  afterEach(async () => {
    if (previousPath === undefined) delete process.env.PATH;
    else process.env.PATH = previousPath;
    await rm(directory, { recursive: true, force: true });
  });

  it('awaits timeout removal even if docker run exits while termination is in progress', async () => {
    const result = await runClientContainer({ ...options, timeoutMs: 1000 });
    assert.equal(result.timedOut, true);
    assert.equal(await readFile(join(directory, 'events'), 'utf8'), 'run\nkill\nremoved\n');
  });

  it('awaits cancellation removal before rejecting with the cancellation reason', async () => {
    const controller = new AbortController();
    const failure = new Error('shutdown');
    const running = runClientContainer({ ...options, signal: controller.signal });
    const rejected = assert.rejects(running, (error) => error === failure);
    const deadline = Date.now() + 3000;
    while (true) {
      try {
        await readFile(join(directory, 'pid'));
        break;
      } catch {
        /* Wait for the child to start. */
      }
      assert.ok(Date.now() < deadline, 'fake Docker did not start');
      await delay(10);
    }
    controller.abort(failure);
    await rejected;
    assert.equal(await readFile(join(directory, 'events'), 'utf8'), 'run\nkill\nremoved\n');
  });

  it('preserves cancellation when container disposal fails and reports cleanup separately', async () => {
    await writeFile(join(directory, 'fail-removal'), '');
    const controller = new AbortController();
    const failure = new Error('shutdown');
    const cleanupErrors: unknown[] = [];
    const running = runClientContainer({
      ...options,
      signal: controller.signal,
      onCleanupError: (error) => cleanupErrors.push(error),
    });
    const rejected = assert.rejects(running, (error) => error === failure);
    const deadline = Date.now() + 3000;
    while (true) {
      try {
        await readFile(join(directory, 'pid'));
        break;
      } catch {
        /* Wait for the child to start. */
      }
      assert.ok(Date.now() < deadline, 'fake Docker did not start');
      await delay(10);
    }
    controller.abort(failure);
    await rejected;
    assert.equal(cleanupErrors.length, 1);
    assert.match(String(cleanupErrors[0]), /permission denied/);
  });

  it('retains the timeout outcome when disposal fails', async () => {
    await writeFile(join(directory, 'fail-removal'), '');
    const cleanupErrors: unknown[] = [];
    const result = await runClientContainer({
      ...options,
      timeoutMs: 1000,
      onCleanupError: (error) => cleanupErrors.push(error),
    });
    assert.equal(result.timedOut, true);
    assert.equal(cleanupErrors.length, 1);
    assert.match(String(cleanupErrors[0]), /permission denied/);
  });

  it('ignores removal of a missing container but reports other removal failures', async () => {
    await writeFile(join(directory, 'missing'), '');
    await removeContainer('missing');
    await writeFile(join(directory, 'fail-removal'), '');
    await assert.rejects(removeContainer('denied'), /permission denied/);
  });

  it('never starts a container for an already cancelled attempt', async () => {
    await assert.rejects(
      runClientContainer({ ...options, signal: AbortSignal.abort(new Error('stopped')) }),
      /stopped/,
    );
    await assert.rejects(readFile(join(directory, 'events')), { code: 'ENOENT' });
  });

  it('clears the deadline after normal exit', async () => {
    const result = await runClientContainer({ ...options, args: ['finish'], timeoutMs: 1000 });
    assert.equal(result.timedOut, false);
    await delay(1100);
    assert.equal(await readFile(join(directory, 'events'), 'utf8'), 'run\n');
  });
});
