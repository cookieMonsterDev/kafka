import type { OutputFormat } from '../output/format';
import { CliUsageError } from './coerce';

export type { OutputFormat } from '../output/format';

export interface GlobalFlags {
  readonly jsonFlag: boolean;
  readonly formatFlag?: OutputFormat;
  readonly quiet: boolean;
  readonly verbosity: number;
  readonly colorFlag: boolean;
  readonly noColorFlag: boolean;
  readonly help: boolean;
  readonly version: boolean;
  /**
   * From `--config-file`/`--config-file=<path>` — an explicit `kafka.config.*` path, overriding
   * discovery. Named `--config-file`, not the shorter `--config`: `topic create` already has its
   * own `--config key=value` (a topic-level config entry), and a global flag may not shadow it.
   */
  readonly configFlag?: string;
  /** From `--profile`/`--profile=<name>` — selects a `cli.profiles` entry. */
  readonly profileFlag?: string;
}

const CONFIG_FLAG_PREFIX = '--config-file=';
const PROFILE_FLAG_PREFIX = '--profile=';

/**
 * Strips every global flag (output format, verbosity, color, `--help`/`-h`, `--version`,
 * `--config-file`, `--profile`) out of `argv`, wherever it appears, so a command's own flag
 * parser never has to know about them — they're reserved names a command may not redeclare (see
 * `RESERVED_FLAG_NAMES`). This runs before any command-specific parsing, including on a path that
 * will end in a usage error, which is what lets that error itself be reported as JSON when
 * `--json`/`--format json` was given.
 */
export function extractGlobalFlags(argv: readonly string[]): { global: GlobalFlags; rest: string[] } {
  let jsonFlag = false;
  let formatFlag: OutputFormat | undefined;
  let quiet = false;
  let verbosity = 0;
  let colorFlag = false;
  let noColorFlag = false;
  let help = false;
  let version = false;
  let configFlag: string | undefined;
  let profileFlag: string | undefined;
  const rest: string[] = [];

  let sawTerminator = false;
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === undefined) continue;
    if (sawTerminator) {
      rest.push(token);
      continue;
    }
    if (token === '--') {
      sawTerminator = true;
      rest.push(token);
      continue;
    }
    if (token.startsWith(CONFIG_FLAG_PREFIX)) {
      configFlag = token.slice(CONFIG_FLAG_PREFIX.length);
      continue;
    }
    if (token.startsWith(PROFILE_FLAG_PREFIX)) {
      profileFlag = token.slice(PROFILE_FLAG_PREFIX.length);
      continue;
    }

    switch (token) {
      case '--json':
        jsonFlag = true;
        continue;
      case '--format=human':
        formatFlag = 'human';
        continue;
      case '--format=json':
        formatFlag = 'json';
        continue;
      case '--format': {
        const value = argv[i + 1];
        if (value !== 'human' && value !== 'json') {
          throw new CliUsageError(
            `--format expects "human" or "json", got ${value === undefined ? 'nothing' : `"${value}"`}`,
          );
        }
        formatFlag = value;
        i += 1;
        continue;
      }
      case '-q':
      case '--quiet':
        quiet = true;
        continue;
      case '-v':
      case '--verbose':
        verbosity += 1;
        continue;
      case '--color':
        colorFlag = true;
        continue;
      case '--no-color':
        noColorFlag = true;
        continue;
      case '-h':
      case '--help':
        help = true;
        continue;
      case '--version':
        version = true;
        continue;
      case '--config-file': {
        const value = argv[i + 1];
        if (value === undefined) {
          throw new CliUsageError('--config-file expects a path');
        }
        configFlag = value;
        i += 1;
        continue;
      }
      case '--profile': {
        const value = argv[i + 1];
        if (value === undefined) {
          throw new CliUsageError('--profile expects a profile name');
        }
        profileFlag = value;
        i += 1;
        continue;
      }
      default:
        rest.push(token);
    }
  }

  return {
    global: {
      jsonFlag,
      formatFlag,
      quiet,
      verbosity,
      colorFlag,
      noColorFlag,
      help,
      version,
      configFlag,
      profileFlag,
    },
    rest,
  };
}
