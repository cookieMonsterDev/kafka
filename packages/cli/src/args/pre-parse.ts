export type OutputFormat = 'human' | 'json';

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
