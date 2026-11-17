import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createLogger, isLogLevelEnabled, LOG_LEVELS, type LogEntry, type Logger } from './index';

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

  describe('isLogLevelEnabled', () => {
    it('follows the current level of the logger and of each namespace', () => {
      const logger = createLogger({ level: LOG_LEVELS.INFO, logCreator });
      const debugNamespace = logger.namespace('Verbose', LOG_LEVELS.DEBUG);

      expect(isLogLevelEnabled(logger, LOG_LEVELS.INFO)).toBe(true);
      expect(isLogLevelEnabled(logger, LOG_LEVELS.DEBUG)).toBe(false);
      expect(isLogLevelEnabled(debugNamespace, LOG_LEVELS.DEBUG)).toBe(true);

      logger.setLogLevel(LOG_LEVELS.DEBUG);
      expect(isLogLevelEnabled(logger, LOG_LEVELS.DEBUG)).toBe(true);
      expect(isLogLevelEnabled(logger.namespace('Inherited'), LOG_LEVELS.DEBUG)).toBe(true);
    });

    it('treats a logger it did not create as enabled at every level', () => {
      const customLogger: Logger = {
        info: () => {},
        error: () => {},
        warn: () => {},
        debug: () => {},
        namespace: () => customLogger,
        setLogLevel: () => {},
      };

      expect(isLogLevelEnabled(customLogger, LOG_LEVELS.DEBUG)).toBe(true);
    });
  });

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
