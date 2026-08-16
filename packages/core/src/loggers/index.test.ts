import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS, type LogEntry } from './index';

describe('loggers', () => {
  let entries: LogEntry[];

  beforeEach(() => {
    entries = [];
    vi.stubEnv('KAFKA_LOG_LEVEL', '');
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  const logCreator = () => (entry: LogEntry) => {
    entries.push(entry);
  };

  it('only logs messages at or below the configured level', () => {
    const logger = createLogger({ level: LOG_LEVELS.WARN, logCreator });

    logger.error('error message');
    logger.warn('warn message');
    logger.info('info message');
    logger.debug('debug message');

    expect(entries.map((e) => e.log.message)).toEqual(['error message', 'warn message']);
  });

  it('lets extra fields override the base log fields', () => {
    const logger = createLogger({ level: LOG_LEVELS.INFO, logCreator });
    logger.info('hello', { message: 'overridden' });

    expect(entries[0]!.log.message).toBe('overridden');
  });

  it('namespaces default to the root log level until given their own', () => {
    const logger = createLogger({ level: LOG_LEVELS.ERROR, logCreator });
    const ns = logger.namespace('MyComponent');

    ns.info('should be filtered out');
    expect(entries).toHaveLength(0);

    logger.setLogLevel(LOG_LEVELS.INFO);
    ns.info('should now pass through');
    expect(entries).toHaveLength(1);
    expect(entries[0]!.namespace).toBe('MyComponent');
  });

  it('a namespace given an explicit level ignores later root-level changes', () => {
    const logger = createLogger({ level: LOG_LEVELS.INFO, logCreator });
    const ns = logger.namespace('MyComponent', LOG_LEVELS.ERROR);

    logger.setLogLevel(LOG_LEVELS.DEBUG);
    ns.info('still filtered, namespace has its own level');

    expect(entries).toHaveLength(0);
  });

  it('KAFKA_LOG_LEVEL overrides the configured level', () => {
    vi.stubEnv('KAFKA_LOG_LEVEL', 'DEBUG');
    const logger = createLogger({ level: LOG_LEVELS.ERROR, logCreator });

    logger.debug('debug message');
    expect(entries).toHaveLength(1);
  });
});
