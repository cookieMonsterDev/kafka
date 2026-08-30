const REDACTED = '[REDACTED]';
const UNREADABLE = '[Unreadable]';

/** Object names (matched case-insensitively) that may carry a secret field. */
const SECRET_BEARING_KEYS = new Set(['sasl', 'ssl']);

/**
 * Every secret-bearing field name across both `sasl` and `ssl`, applied uniformly to whichever of
 * the two a value is found under — not two separate per-key allow-lists. Neither shape has a field
 * whose name collides with the other's secret fields, so this costs nothing in the ordinary case,
 * and it is what keeps redaction correct if the exact same object is ever reachable as both a
 * `sasl` value and an `ssl` value (redacting it once under one key's narrower list would otherwise
 * leave the other key's secret exposed under the alias).
 */
const SECRET_FIELDS = new Set(['password', 'secretAccessKey', 'sessionToken', 'tokenHmac', 'key', 'pfx', 'passphrase']);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) && !Buffer.isBuffer(value);
}

/** A property access safe against a getter that throws — reported as `'[Unreadable]'` rather than propagating. */
function tryGet(source: Record<string, unknown>, key: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: source[key] };
  } catch {
    return { ok: false };
  }
}

/**
 * `stack` tracks objects currently being recursed into — not every object ever seen — so a real
 * cycle (an object reachable from itself) is still caught, but the same object legitimately
 * reachable via two independent, non-cyclic paths is redacted correctly on each path instead of
 * being flattened to `'[Circular]'` the second time.
 */
function redactSection(section: Record<string, unknown>, stack: Set<object>): Record<string, unknown> | '[Circular]' {
  if (stack.has(section)) return '[Circular]';
  stack.add(section);
  try {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(section)) {
      const got = tryGet(section, key);
      if (!got.ok) {
        out[key] = UNREADABLE;
        continue;
      }
      out[key] = SECRET_FIELDS.has(key) && got.value !== undefined ? REDACTED : redactValue(got.value, stack);
    }
    return out;
  } finally {
    stack.delete(section);
  }
}

function redactSecretBearing(value: unknown, stack: Set<object>): unknown {
  if (isPlainObject(value)) return redactSection(value, stack);
  if (Array.isArray(value)) return value.map((item) => redactSecretBearing(item, stack));
  return redactValue(value, stack);
}

function redactValue(value: unknown, stack: Set<object>): unknown {
  if (typeof value === 'function') return '[Function]';
  if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
  if (value === null || typeof value !== 'object') return value;

  if (stack.has(value)) return '[Circular]';
  stack.add(value);
  try {
    if (Array.isArray(value)) return value.map((item) => redactValue(item, stack));

    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value)) {
      const got = tryGet(value as Record<string, unknown>, key);
      if (!got.ok) {
        out[key] = UNREADABLE;
        continue;
      }
      out[key] = SECRET_BEARING_KEYS.has(key.toLowerCase())
        ? redactSecretBearing(got.value, stack)
        : redactValue(got.value, stack);
    }
    return out;
  } finally {
    stack.delete(value);
  }
}

/**
 * Deep-clones any value with secrets replaced by `'[REDACTED]'`, safe to pass straight to
 * `JSON.stringify` — functions become `'[Function]'`, `Buffer`s become `'[Buffer N bytes]'`, a
 * circular reference becomes `'[Circular]'`, and a property whose getter throws becomes
 * `'[Unreadable]'` — none of these throw.
 *
 * The redacted fields are an explicit allow-list: `password`, `secretAccessKey`, `sessionToken`,
 * `tokenHmac`, `key`, `pfx`, `passphrase` — applied wherever a `sasl` or `ssl` object (or array of
 * them), matched case-insensitively, appears at any depth (so this also redacts a `kafka.config.*`
 * file's `client.sasl`/`client.ssl`, not only an already-resolved `KafkaConfig`).
 *
 * Apply this at every boundary a config value crosses into output: diagnostics, error messages,
 * and any printer — never log or serialize an unredacted config.
 */
export function redactKafkaConfig(value: unknown): unknown {
  return redactValue(value, new Set());
}
