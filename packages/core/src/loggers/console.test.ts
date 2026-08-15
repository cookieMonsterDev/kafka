import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { consoleLogCreator } from './console.js'
import { LOG_LEVELS, type LogEntry } from './index.js'

describe('loggers/console', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  const entry = (level: LogEntry['level'], label: string, namespace?: string): LogEntry => ({
    namespace,
    level,
    label,
    log: { timestamp: '2024-01-01T00:00:00.000Z', logger: 'kafkajs', message: 'hello' },
  })

  it('routes each level to the matching console method', () => {
    const log = consoleLogCreator(LOG_LEVELS.INFO)

    log(entry(LOG_LEVELS.INFO, 'INFO'))
    log(entry(LOG_LEVELS.ERROR, 'ERROR'))
    log(entry(LOG_LEVELS.WARN, 'WARN'))
    log(entry(LOG_LEVELS.DEBUG, 'DEBUG'))

    expect(console.info).toHaveBeenCalledTimes(1)
    expect(console.error).toHaveBeenCalledTimes(1)
    expect(console.warn).toHaveBeenCalledTimes(1)
    expect(console.log).toHaveBeenCalledTimes(1)
  })

  it('prefixes the message with the namespace', () => {
    const log = consoleLogCreator(LOG_LEVELS.INFO)
    log(entry(LOG_LEVELS.INFO, 'INFO', 'MyComponent'))

    const [payload] = vi.mocked(console.info).mock.calls[0]!
    expect(JSON.parse(payload as string).message).toBe('[MyComponent] hello')
  })

  it('serializes level as the human-readable label', () => {
    const log = consoleLogCreator(LOG_LEVELS.INFO)
    log(entry(LOG_LEVELS.INFO, 'INFO'))

    const [payload] = vi.mocked(console.info).mock.calls[0]!
    expect(JSON.parse(payload as string).level).toBe('INFO')
  })
})
