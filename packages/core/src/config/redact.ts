const REDACTED = '[REDACTED]';

/** Secret-bearing field names, keyed by the object they live under. An explicit allow-list, not a name heuristic. */
const SECRET_FIELDS: Readonly<Record<string, ReadonlySet<string>>> = {
  sasl: new Set(['password', 'secretAccessKey', 'sessionToken', 'tokenHmac']),
  ssl: new Set(['key', 'pfx', 'passphrase']),
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) && !Buffer.isBuffer(value);
}

function redactSection(
  section: Record<string, unknown>,
  secretFields: ReadonlySet<string>,
  seen: WeakSet<object>,
): unknown {
  if (seen.has(section)) return '[Circular]';
  seen.add(section);

  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(section)) {
    out[key] = secretFields.has(key) && value !== undefined ? REDACTED : redactValue(value, seen);
  }
  return out;
}

function redactValue(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === 'function') return '[Function]';
  if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
  if (value === null || typeof value !== 'object') return value;

  if (seen.has(value)) return '[Circular]';
  seen.add(value);

  if (Array.isArray(value)) return value.map((item) => redactValue(item, seen));

  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    const secretFields = SECRET_FIELDS[key];
    out[key] =
      secretFields !== undefined && isPlainObject(entry)
        ? redactSection(entry, secretFields, seen)
        : redactValue(entry, seen);
  }
  return out;
}

/**
 * Deep-clones any value with secrets replaced by `'[REDACTED]'`, safe to pass straight to
 * `JSON.stringify` — functions become `'[Function]'`, `Buffer`s become `'[Buffer N bytes]'`, and a
 * circular reference becomes `'[Circular]'` instead of throwing.
 *
 * The redacted fields are an explicit allow-list (D10): `sasl.password`, `sasl.secretAccessKey`,
 * `sasl.sessionToken`, `sasl.tokenHmac`, `ssl.key`, `ssl.pfx`, `ssl.passphrase` — wherever a `sasl`
 * or `ssl` object appears, at any depth (so this also redacts a `kafka.config.*` file's
 * `client.sasl`/`client.ssl`, not only a already-resolved `KafkaConfig`).
 *
 * Apply this at every boundary a config value crosses into output: diagnostics, error messages,
 * and any printer — never log or serialize an unredacted config.
 */
export function redactKafkaConfig(value: unknown): unknown {
  return redactValue(value, new WeakSet());
}
