import { createPalette } from './colors';
import { createLogger, type CliLogLevel } from './logger';
import type { CliError } from '../errors/cli-error';

export type OutputFormat = 'human' | 'json';

export interface ResolveOutputFormatInput {
  readonly jsonFlag: boolean;
  readonly formatFlag?: OutputFormat;
  readonly env: Readonly<Record<string, string | undefined>>;
}

const OUTPUT_ENV_VAR = 'KAFKA_OUTPUT';

/** `--json` and an explicit `--format` both beat `KAFKA_OUTPUT`, which beats the human default. */
export function resolveOutputFormat(input: ResolveOutputFormatInput): OutputFormat {
  if (input.jsonFlag) return 'json';
  if (input.formatFlag !== undefined) return input.formatFlag;
  if (input.env[OUTPUT_ENV_VAR] === 'json') return 'json';
  return 'human';
}

export interface Rendered {
  readonly human: () => string;
  readonly json: () => string;
}

/**
 * Writes exactly one rendering to stdout, chosen by `format` — never both, and the unused
 * renderer is never even called.
 */
export function writeFormatted(
  stdout: { write(chunk: string): unknown },
  format: OutputFormat,
  rendered: Rendered,
): void {
  const text = format === 'json' ? rendered.json() : rendered.human();
  stdout.write(`${text}\n`);
}

/**
 * Reports a failure: as the one JSON document on stdout when `format` is `json` (so a script
 * gets a consistent, parseable shape even on failure), otherwise as a plain line on stderr,
 * alongside every other diagnostic.
 */
export function writeError(
  streams: { stdout: { write(chunk: string): unknown }; stderr: { write(chunk: string): unknown } },
  format: OutputFormat,
  message: string,
): void {
  if (format === 'json') {
    streams.stdout.write(`${JSON.stringify({ error: { message } })}\n`);
  } else {
    streams.stderr.write(`kafka: ${message}\n`);
  }
}

/**
 * Reports a full {@link CliError}: as the one JSON document on stdout when `format` is `json`
 * (including any sub-`items`, e.g. a flattened `KafkaAggregateError`), otherwise as a message
 * plus one indented line per item on stderr.
 */
export function writeCliError(
  streams: { stdout: { write(chunk: string): unknown }; stderr: { write(chunk: string): unknown } },
  format: OutputFormat,
  error: CliError,
): void {
  if (format === 'json') {
    streams.stdout.write(`${JSON.stringify({ error: { message: error.message, items: error.items } })}\n`);
    return;
  }
  const lines = [`kafka: ${error.message}`, ...(error.items ?? []).map((item) => `  - ${item.message}`)];
  streams.stderr.write(`${lines.join('\n')}\n`);
}

export interface CreateCommandOutputInput {
  readonly stdout: { write(chunk: string): unknown };
  readonly stderr: { write(chunk: string): unknown };
  readonly format: OutputFormat;
  readonly useColor: boolean;
  readonly logLevel: CliLogLevel;
}

/** Builds the `output` a command receives on its {@link CommandContext} — see `args/define.ts`. */
export function createCommandOutput(input: CreateCommandOutputInput) {
  const palette = createPalette(input.useColor);
  const log = createLogger(input.stderr, input.logLevel);
  return {
    palette,
    log,
    write(rendered: Rendered) {
      writeFormatted(input.stdout, input.format, rendered);
    },
    error(message: string) {
      writeError({ stdout: input.stdout, stderr: input.stderr }, input.format, message);
    },
    cliError(error: CliError) {
      writeCliError({ stdout: input.stdout, stderr: input.stderr }, input.format, error);
    },
  };
}
