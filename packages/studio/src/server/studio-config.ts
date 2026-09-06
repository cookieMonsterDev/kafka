/**
 * The `studio:` section of a `kafka.config.*` file — the same file the rest of this project's
 * tooling already reads, extended with this package's own defaults. An older reader never
 * rejects a file carrying a section it doesn't know about yet, and this one never rejects a key
 * inside `studio:` it doesn't recognize either — both warn, never throw.
 */
export interface StudioFileConfig {
  readonly port?: number;
  readonly host?: string;
  readonly openBrowser?: boolean;
  readonly readOnly?: boolean;
  readonly maxTail?: number;
}

const KNOWN_KEYS: ReadonlySet<string> = new Set(['port', 'host', 'openBrowser', 'readOnly', 'maxTail']);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readPort(raw: unknown, warn: (message: string) => void): number | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'number' && Number.isInteger(raw) && raw >= 1 && raw <= 65535) return raw;
  warn('"studio.port" must be an integer between 1 and 65535; ignoring it');
  return undefined;
}

function readHost(raw: unknown, warn: (message: string) => void): string | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'string' && raw.length > 0) return raw;
  warn('"studio.host" must be a non-empty string; ignoring it');
  return undefined;
}

function readBoolean(raw: unknown, key: string, warn: (message: string) => void): boolean | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'boolean') return raw;
  warn(`"studio.${key}" must be a boolean; ignoring it`);
  return undefined;
}

function readMaxTail(raw: unknown, warn: (message: string) => void): number | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'number' && Number.isInteger(raw) && raw > 0) return raw;
  warn('"studio.maxTail" must be a positive integer; ignoring it');
  return undefined;
}

/**
 * Reads the `studio:` section of a raw, already-loaded config file object. Never throws — an
 * unknown top-level key inside `studio:` (or a known key holding the wrong shape) is reported
 * through `warn` and otherwise ignored, so a config file written for a newer studio still loads
 * under an older one.
 */
export function readStudioSection(
  fileConfig: Record<string, unknown> | null,
  warn: (message: string) => void = () => {},
): StudioFileConfig {
  const raw = fileConfig?.studio;
  if (raw === undefined) return {};
  if (!isPlainObject(raw)) {
    warn('the "studio" section of the config file must be an object; ignoring it');
    return {};
  }

  for (const key of Object.keys(raw)) {
    if (!KNOWN_KEYS.has(key)) {
      warn(`unknown "studio.${key}" in the config file; ignoring it`);
    }
  }

  return {
    port: readPort(raw.port, warn),
    host: readHost(raw.host, warn),
    openBrowser: readBoolean(raw.openBrowser, 'openBrowser', warn),
    readOnly: readBoolean(raw.readOnly, 'readOnly', warn),
    maxTail: readMaxTail(raw.maxTail, warn),
  };
}
