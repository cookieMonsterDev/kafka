import type { ResolvedCliConfig } from '../config/resolve';
import type { CliError } from '../errors/cli-error';
import type { Palette } from '../output/colors';
import type { Rendered } from '../output/format';
import type { CliLogger } from '../output/logger';
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

export interface CommandOutput {
  readonly palette: Palette;
  readonly log: CliLogger;
  /** Writes exactly one of `human`/`json` to stdout, chosen by the resolved output format. */
  write(rendered: Rendered): void;
  /** Reports a failure — JSON on stdout when the format is JSON, otherwise a line on stderr. */
  error(message: string): void;
  /** Reports a full {@link CliError}, including any sub-items (a flattened aggregate error). */
  cliError(error: CliError): void;
}

export interface CommandContext {
  readonly runtime: Runtime;
  readonly flags: Readonly<Record<string, unknown>>;
  readonly positionals: readonly string[];
  readonly output: CommandOutput;
  /** This invocation's already-resolved `kafka.config.*` file — see `config/resolve.ts`. */
  readonly config: ResolvedCliConfig;
}

export interface CommandSpec {
  readonly path: readonly string[];
  readonly summary: string;
  readonly flags?: readonly FlagSpec[];
  readonly positionals?: readonly PositionalSpec[];
  readonly examples?: readonly string[];
  /** Every exit code this command can resolve to — validated against the shared taxonomy at mount time. */
  readonly exitCodes: readonly number[];
  /**
   * Marks a command whose argument/result shape tracks core's own types too closely to hold a
   * stable contract — shown in its help output so a caller knows not to script against it as
   * confidently as a command with a frozen shape.
   */
  readonly unstable?: boolean;
  readonly run: (context: CommandContext) => Promise<number>;
}
