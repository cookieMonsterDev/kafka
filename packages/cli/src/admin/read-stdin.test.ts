import { describe, expect, it } from 'vitest';
import { createFakeStdin } from '../testing/create-fake-stdin';
import { readStdinToEnd } from './read-stdin';

describe('readStdinToEnd', () => {
  it('resolves the full input once stdin ends', async () => {
    const stdin = createFakeStdin();
    const promise = readStdinToEnd(stdin);
    stdin.emitData('hunter2');
    stdin.emitEnd();
    await expect(promise).resolves.toBe('hunter2');
  });

  it('accumulates chunks split across multiple data events', async () => {
    const stdin = createFakeStdin();
    const promise = readStdinToEnd(stdin);
    stdin.emitData('hun');
    stdin.emitData('ter');
    stdin.emitData('2');
    stdin.emitEnd();
    await expect(promise).resolves.toBe('hunter2');
  });

  it('trims exactly one trailing newline', async () => {
    const stdin = createFakeStdin();
    const promise = readStdinToEnd(stdin);
    stdin.emitData('hunter2\n');
    stdin.emitEnd();
    await expect(promise).resolves.toBe('hunter2');
  });

  it('trims only the single trailing newline, keeping any others', async () => {
    const stdin = createFakeStdin();
    const promise = readStdinToEnd(stdin);
    stdin.emitData('hunter2\n\n');
    stdin.emitEnd();
    await expect(promise).resolves.toBe('hunter2\n');
  });

  it('resolves an empty string for empty input', async () => {
    const stdin = createFakeStdin();
    const promise = readStdinToEnd(stdin);
    stdin.emitEnd();
    await expect(promise).resolves.toBe('');
  });

  it('does not resolve before "end" fires, even after data', async () => {
    const stdin = createFakeStdin();
    let resolved = false;
    const promise = readStdinToEnd(stdin).then((value) => {
      resolved = true;
      return value;
    });
    stdin.emitData('hunter2');
    await Promise.resolve();
    expect(resolved).toBe(false);
    stdin.emitEnd();
    await expect(promise).resolves.toBe('hunter2');
  });

  it('sets the encoding to utf8 before reading', () => {
    const stdin = createFakeStdin();
    void readStdinToEnd(stdin);
    expect(stdin.setEncoding).toHaveBeenCalledWith('utf8');
  });
});
