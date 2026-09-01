import type { KafkaConfigSource } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { buildConnectionOverrides } from '../../config/connection';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

interface DoctorReport {
  readonly configPath: string | null;
  readonly transformFallbackUsed: boolean;
  readonly activeProfile: string | null;
  readonly availableProfiles: readonly string[];
  readonly brokersResolved: boolean;
  readonly connectionError?: string;
  readonly configSource?: KafkaConfigSource;
}

function hasName(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}

function renderReport(report: DoctorReport): string {
  const lines = [
    `config file       ${report.configPath ?? '(none found)'}`,
    `transform rescue  ${report.transformFallbackUsed ? 'used' : 'not used'}`,
    `active profile    ${report.activeProfile ?? '(none)'}`,
    `known profiles    ${report.availableProfiles.length > 0 ? report.availableProfiles.join(', ') : '(none)'}`,
    `brokers resolved  ${report.brokersResolved ? 'yes' : `no — ${report.connectionError ?? 'unknown'}`}`,
  ];

  if (report.configSource !== undefined) {
    const rows = Object.entries(report.configSource.keys).map(([key, source]) => [key, source]);
    lines.push('', renderTable(['KEY', 'SOURCE'], rows));
  }

  return lines.join('\n');
}

export const doctorCommand: CommandSpec = {
  path: ['doctor'],
  summary: 'Report where connection settings come from — config file, environment, profile, or flag',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, to diagnose as if given' }],
  examples: ['doctor', 'doctor --profile staging'],
  exitCodes: [EXIT_CODES.ok],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);

    // Doctor is a diagnostic, not a connecting command, but it needs core's Kafka-typed facade
    // (`fromEnv`, `Kafka.from`, `configSource()`) to report anything meaningful — lazily imported
    // here, same as `admin/open.ts`, so every other command still never pays for it.
    const { Kafka, fromEnv } = await import('@cookiemonsterdev/kafka-core');
    const envOverrides = fromEnv(runtime.env);
    const overrides = buildConnectionOverrides({ brokers }, envOverrides, config);

    let brokersResolved = true;
    let connectionError: string | undefined;
    let configSource: KafkaConfigSource | undefined;
    try {
      const kafka = Kafka.from(config.fileConfig ?? {}, overrides);
      configSource = kafka.configSource();
    } catch (error) {
      if (!hasName(error, 'KafkaConfigError')) throw error;
      brokersResolved = false;
      connectionError = error instanceof Error ? error.message : String(error);
    }

    const report: DoctorReport = {
      configPath: config.path,
      transformFallbackUsed: config.transformFallbackUsed,
      activeProfile: config.profile,
      availableProfiles: Object.keys(config.cli.profiles ?? {}),
      brokersResolved,
      connectionError,
      configSource,
    };

    output.write({
      human: () => renderReport(report),
      json: () => stringifyJsonSafe(report),
    });
    return EXIT_CODES.ok;
  },
};
