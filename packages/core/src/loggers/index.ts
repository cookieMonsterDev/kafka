export const LOG_LEVELS = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
} as const

export type LogLevel = (typeof LOG_LEVELS)[keyof typeof LOG_LEVELS]

export interface LogEntry {
  namespace: string | undefined
  level: LogLevel
  label: string
  log: {
    timestamp: string
    logger: string
    message: string
    [key: string]: unknown
  }
}

export type LogCreator = (logLevel: LogLevel) => (entry: LogEntry) => void

export interface Logger {
  info(message: string, extra?: Record<string, unknown>): void
  error(message: string, extra?: Record<string, unknown>): void
  warn(message: string, extra?: Record<string, unknown>): void
  debug(message: string, extra?: Record<string, unknown>): void
  namespace(namespace: string, logLevel?: LogLevel | null): Logger
  setLogLevel(newLevel: LogLevel): void
}

export interface LoggerConfig {
  level?: LogLevel
  logCreator: LogCreator
}

function createLevel(
  label: string,
  level: LogLevel,
  currentLevel: () => LogLevel,
  namespace: string | undefined,
  logFunction: (entry: LogEntry) => void
) {
  return (message: string, extra: Record<string, unknown> = {}): void => {
    if (level > currentLevel()) return

    logFunction({
      namespace,
      level,
      label,
      log: {
        timestamp: new Date().toISOString(),
        logger: 'kafkajs',
        message,
        ...extra,
      },
    })
  }
}

function evaluateLogLevel(level: LogLevel): LogLevel
function evaluateLogLevel(level: LogLevel | null): LogLevel | null
function evaluateLogLevel(level: LogLevel | null): LogLevel | null {
  const envLogLevel = (process.env.KAFKAJS_LOG_LEVEL ?? '').toUpperCase()
  const envLevel = (LOG_LEVELS as Record<string, LogLevel | undefined>)[envLogLevel]
  return envLevel == null ? level : envLevel
}

export function createLogger({ level = LOG_LEVELS.INFO, logCreator }: LoggerConfig): Logger {
  let logLevel: LogLevel = evaluateLogLevel(level)
  const logFunction = logCreator(logLevel)

  const createNamespace = (namespace: string, namespaceLevel: LogLevel | null = null): Logger =>
    createLogFunctions(namespace, evaluateLogLevel(namespaceLevel))

  const createLogFunctions = (namespace?: string, namespaceLogLevel: LogLevel | null = null): Logger => {
    const currentLogLevel = (): LogLevel => (namespaceLogLevel == null ? logLevel : namespaceLogLevel)

    return {
      info: createLevel('INFO', LOG_LEVELS.INFO, currentLogLevel, namespace, logFunction),
      error: createLevel('ERROR', LOG_LEVELS.ERROR, currentLogLevel, namespace, logFunction),
      warn: createLevel('WARN', LOG_LEVELS.WARN, currentLogLevel, namespace, logFunction),
      debug: createLevel('DEBUG', LOG_LEVELS.DEBUG, currentLogLevel, namespace, logFunction),
      namespace: createNamespace,
      setLogLevel: (newLevel: LogLevel) => {
        logLevel = newLevel
      },
    }
  }

  return createLogFunctions()
}
