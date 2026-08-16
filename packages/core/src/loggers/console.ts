import { LOG_LEVELS, type LogCreator, type LogEntry } from './index';

export const consoleLogCreator: LogCreator = () => {
  return (entry: LogEntry) => {
    const prefix = entry.namespace ? `[${entry.namespace}] ` : '';
    const message = JSON.stringify(
      {
        level: entry.label,
        ...entry.log,
        message: `${prefix}${entry.log.message}`,
      },
      (_key: string, value: unknown) => (typeof value === 'bigint' ? value.toString() : value),
    );

    switch (entry.level) {
      case LOG_LEVELS.INFO:
        return console.info(message);
      case LOG_LEVELS.ERROR:
        return console.error(message);
      case LOG_LEVELS.WARN:
        return console.warn(message);
      case LOG_LEVELS.DEBUG:
        return console.log(message);
    }
  };
};
