import type { Runtime } from '../runtime';

export type FlagType = 'string' | 'boolean' | 'number' | 'enum';

export interface FlagSpec {
  readonly name: string;
  readonly type: FlagType;
  /** Single-character shorthand, e.g. `p` for `--partitions`/`-p`. */
  readonly alias?: string;
  /** Repeatable (`--tag a --tag b` → `['a', 'b']`). */
  readonly multiple?: boolean;
  /** Boolean only: accepts `--no-<name>` to force it to `false`. */
  readonly negatable?: boolean;
  /** Enum only: the closed set of accepted values. */
  readonly values?: readonly string[];
  /** String + multiple only: each value must be `key=value`, decoded into a record. */
  readonly keyValue?: boolean;
  readonly brief: string;
}

export interface PositionalSpec {
  readonly name: string;
  readonly variadic?: boolean;
  readonly brief: string;
}

export interface CommandContext {
  readonly runtime: Runtime;
  readonly flags: Readonly<Record<string, unknown>>;
  readonly positionals: readonly string[];
}

export interface CommandSpec {
  readonly path: readonly string[];
  readonly summary: string;
  readonly flags?: readonly FlagSpec[];
  readonly positionals?: readonly PositionalSpec[];
  readonly examples?: readonly string[];
  /** Every exit code this command can resolve to — validated against the shared taxonomy at mount time. */
  readonly exitCodes: readonly number[];
  readonly run: (context: CommandContext) => Promise<number>;
}
