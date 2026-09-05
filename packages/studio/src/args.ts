import { parseArgs } from 'node:util';

export class StudioUsageError extends Error {}

export interface StudioArgs {
  readonly port?: number;
  readonly host?: string;
  /** `undefined` defers to `BROWSER`/the platform default; `'none'` disables opening entirely. */
  readonly browser?: string;
  readonly readOnly: boolean;
  readonly help: boolean;
  readonly version: boolean;
}

function coercePort(raw: string): number {
  const port = Number.parseInt(raw, 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535 || String(port) !== raw.trim()) {
    throw new StudioUsageError(`invalid --port value "${raw}" — expected an integer between 1 and 65535`);
  }
  return port;
}

/** Parses `kafka-studio`'s flat flag set. Throws {@link StudioUsageError} for anything a user got wrong. */
export function parseStudioArgs(argv: readonly string[]): StudioArgs {
  let raw: ReturnType<typeof parseArgs>;
  try {
    raw = parseArgs({
      args: argv,
      options: {
        port: { type: 'string', short: 'p' },
        host: { type: 'string' },
        browser: { type: 'string' },
        'no-browser': { type: 'boolean' },
        'read-only': { type: 'boolean' },
        help: { type: 'boolean', short: 'h' },
        version: { type: 'boolean', short: 'v' },
      },
      allowPositionals: false,
      strict: true,
    });
  } catch (error) {
    throw new StudioUsageError(error instanceof Error ? error.message : String(error));
  }

  const values = raw.values as {
    port?: string;
    host?: string;
    browser?: string;
    'no-browser'?: boolean;
    'read-only'?: boolean;
    help?: boolean;
    version?: boolean;
  };
  const browser = values.browser ?? (values['no-browser'] === true ? 'none' : undefined);

  return {
    ...(values.port !== undefined ? { port: coercePort(values.port) } : {}),
    ...(values.host !== undefined ? { host: values.host } : {}),
    ...(browser !== undefined ? { browser } : {}),
    readOnly: values['read-only'] === true,
    help: values.help === true,
    version: values.version === true,
  };
}

export function usageText(): string {
  return `Usage: kafka-studio [options]

Launches a local web UI for inspecting and driving a Kafka cluster.

Options:
  -p, --port <number>   port to listen on (default: first free port in 5757-5807)
      --host <address>  address to bind (default: 127.0.0.1)
      --browser <cmd>   browser command to open, or "none" to disable
      --no-browser      shorthand for --browser none
      --read-only       reject mutating API requests with 403
  -h, --help             print this message
  -v, --version          print the installed version
`;
}
