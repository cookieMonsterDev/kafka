import type { OutputFormat } from '../output/format';

export type { OutputFormat } from '../output/format';

/**
 * A cheap, tolerant scan of the raw argv for `--json`/`--format`, done before the real parser
 * runs. A usage error (an unknown flag, a bad enum value, …) needs to know up front whether it
 * should itself be emitted as JSON — by the time the real parser has thrown, it's too late to
 * ask it.
 */
export function preParseOutputFormat(argv: readonly string[]): OutputFormat {
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--') break;
    if (token === '--json') return 'json';
    if (token === '--format=json') return 'json';
    if (token === '--format' && argv[i + 1] === 'json') return 'json';
  }
  return 'human';
}

export interface GlobalFlags {
  readonly jsonFlag: boolean;
  readonly formatFlag?: OutputFormat;
  readonly quiet: boolean;
  readonly verbosity: number;
  readonly colorFlag: boolean;
  readonly noColorFlag: boolean;
  readonly help: boolean;
  readonly version: boolean;
}

/**
 * Strips every global flag (output format, verbosity, color, `--help`/`-h`, `--version`) out of
 * `argv`, wherever it appears, so a command's own flag parser never has to know about them —
 * they're reserved names a command may not redeclare (see `RESERVED_FLAG_NAMES`).
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
      case '--format':
        formatFlag = argv[i + 1] === 'json' ? 'json' : 'human';
        i += 1;
        continue;
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
      default:
        rest.push(token);
    }
  }

  return { global: { jsonFlag, formatFlag, quiet, verbosity, colorFlag, noColorFlag, help, version }, rest };
}
