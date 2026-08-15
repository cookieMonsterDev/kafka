import { LOG_LEVELS, type LogCreator, type LogEntry } from './index.js'

export const consoleLogCreator: LogCreator = () => {
  return (entry: LogEntry) => {
    const prefix = entry.namespace ? `[${entry.namespace}] ` : ''
    const message = JSON.stringify({
      level: entry.label,
      ...entry.log,
      message: `${prefix}${entry.log.message}`,
    })

    switch (entry.level) {
      case LOG_LEVELS.INFO:
        return console.info(message)
      case LOG_LEVELS.ERROR:
        return console.error(message)
      case LOG_LEVELS.WARN:
        return console.warn(message)
      case LOG_LEVELS.DEBUG:
        return console.log(message)
    }
  }
}
