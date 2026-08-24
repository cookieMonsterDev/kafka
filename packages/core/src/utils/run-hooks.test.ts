import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { runHooks } from './run-hooks';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('utils/runHooks', () => {
  it('does nothing when hooks is undefined or empty', async () => {
    await expect(runHooks(undefined, { a: 1 }, 'onTest', silentLogger)).resolves.toBeUndefined();
    await expect(runHooks([], { a: 1 }, 'onTest', silentLogger)).resolves.toBeUndefined();
  });

  it('runs hooks in registration order, awaiting each before the next starts', async () => {
    const order: string[] = [];
    let releaseFirst!: () => void;
    const first = vi.fn(async () => {
      order.push('first-start');
      await new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });
      order.push('first-end');
    });
    const second = vi.fn(async () => {
      order.push('second');
    });

    const done = runHooks([first, second], { value: 42 }, 'onTest', silentLogger);
    await Promise.resolve();
    await Promise.resolve();
    expect(order).toEqual(['first-start']);

    releaseFirst();
    await done;

    expect(order).toEqual(['first-start', 'first-end', 'second']);
    expect(first).toHaveBeenCalledWith({ value: 42 });
    expect(second).toHaveBeenCalledWith({ value: 42 });
  });

  it('catches a synchronously throwing hook, logs it, and still runs the next hook', async () => {
    const errorLog = vi.fn();
    const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
    logger.error = errorLog;

    const throwing = vi.fn(() => {
      throw new Error('boom');
    });
    const next = vi.fn(async () => undefined);

    await expect(runHooks([throwing, next], {}, 'onSend', logger)).resolves.toBeUndefined();

    expect(throwing).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenCalledTimes(1);
    expect(errorLog).toHaveBeenCalledWith(
      expect.stringContaining('onSend'),
      expect.objectContaining({ error: 'boom' }),
    );
  });

  it('catches a hook that rejects and still runs the next hook', async () => {
    const errorLog = vi.fn();
    const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
    logger.error = errorLog;

    const rejecting = vi.fn(async () => {
      throw new Error('async boom');
    });
    const next = vi.fn(async () => undefined);

    await expect(runHooks([rejecting, next], {}, 'onAck', logger)).resolves.toBeUndefined();

    expect(rejecting).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenCalledTimes(1);
    expect(errorLog).toHaveBeenCalledWith(
      expect.stringContaining('onAck'),
      expect.objectContaining({ error: 'async boom' }),
    );
  });

  it('composes multiple hooks, all receiving the same event', async () => {
    const seen: unknown[] = [];
    const hooks = [1, 2, 3].map((n) =>
      vi.fn((event: { value: number }) => {
        seen.push([n, event.value]);
      }),
    );

    await runHooks(hooks, { value: 7 }, 'onTest', silentLogger);

    expect(seen).toEqual([
      [1, 7],
      [2, 7],
      [3, 7],
    ]);
  });
});
