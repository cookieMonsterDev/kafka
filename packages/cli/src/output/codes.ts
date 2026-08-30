/**
 * Local copies of core's protocol enum tables, not imports of them: a static value import would
 * drag core's dist into every invocation, including `--help`/`--version`. Each table is checked
 * against core's real export by a test that imports core dynamically, so a table can't drift.
 */
export const CONFIG_RESOURCE_TYPES = Object.freeze({
  UNKNOWN: 0,
  TOPIC: 2,
  BROKER: 4,
  BROKER_LOGGER: 8,
  CLIENT_METRICS: 16,
  GROUP: 32,
});

export interface DescribedCode {
  readonly name: string | null;
  readonly code: number;
}

/** Resolves a numeric protocol code against a name table, for a `{name, code}` JSON shape. */
export function describeCode(table: Readonly<Record<string, number>>, code: number): DescribedCode {
  for (const [name, value] of Object.entries(table)) {
    if (value === code) return { name, code };
  }
  return { name: null, code };
}

/** Human-readable rendering of a {@link DescribedCode} — `UNKNOWN(N)` when nothing matched. */
export function formatCode(described: DescribedCode): string {
  return described.name ?? `UNKNOWN(${String(described.code)})`;
}
