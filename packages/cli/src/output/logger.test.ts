import { describe, expect, it, vi } from 'vitest';
import { CLI_LOG_LEVELS, createLogger, verbosityToLogLevel } from './logger';

describe('CLI_LOG_LEVELS', () => {
  it('matches core LOG_LEVELS', async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CLI_LOG_LEVELS).toEqual(core.logLevel);
  });
});

describe('verbosityToLogLevel', () => {
  it('defaults to WARN', () => {
    expect(verbosityToLogLevel(false, 0)).toBe(CLI_LOG_LEVELS.WARN);
  });

  it('-q lowers to ERROR regardless of verbosity', () => {
    expect(verbosityToLogLevel(true, 0)).toBe(CLI_LOG_LEVELS.ERROR);
    expect(verbosityToLogLevel(true, 2)).toBe(CLI_LOG_LEVELS.ERROR);
  });

  it('one -v raises to INFO', () => {
    expect(verbosityToLogLevel(false, 1)).toBe(CLI_LOG_LEVELS.INFO);
  });

  it('two or more -v raise to DEBUG', () => {
    expect(verbosityToLogLevel(false, 2)).toBe(CLI_LOG_LEVELS.DEBUG);
    expect(verbosityToLogLevel(false, 3)).toBe(CLI_LOG_LEVELS.DEBUG);
  });
});

describe('createLogger', () => {
  it('writes warn at the WARN level but not info or debug', () => {
    const write = vi.fn((_chunk: string) => true);
    const logger = createLogger({ write }, CLI_LOG_LEVELS.WARN);

    logger.warn('a warning');
    logger.info('some info');
    logger.debug('some debug');

    expect(write).toHaveBeenCalledTimes(1);
    expect(write).toHaveBeenCalledWith('a warning\n');
  });

  it('writes nothing at ERROR level', () => {
    const write = vi.fn((_chunk: string) => true);
    const logger = createLogger({ write }, CLI_LOG_LEVELS.ERROR);

    logger.warn('a warning');
    logger.info('some info');
    logger.debug('some debug');

    expect(write).not.toHaveBeenCalled();
  });

  it('writes everything at DEBUG level', () => {
    const write = vi.fn((_chunk: string) => true);
    const logger = createLogger({ write }, CLI_LOG_LEVELS.DEBUG);

    logger.warn('a warning');
    logger.info('some info');
    logger.debug('some debug');

    expect(write).toHaveBeenCalledTimes(3);
  });
});
