import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { CONFIG_OPERATIONS } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';
import { resolveConfigResourceType, type ConfigResourceType } from './resource-type';

const CONCURRENCY = 8;

interface ResourceResult {
  readonly resource: string;
  readonly ok: boolean;
  readonly detail?: string;
}

async function unsetOne(
  admin: Admin,
  type: ConfigResourceType,
  name: string,
  keys: readonly string[],
  validateOnly: boolean,
): Promise<ResourceResult> {
  await admin.incrementalAlterConfigs({
    resources: [
      {
        type,
        name,
        configs: keys.map((key) => ({ name: key, value: null, operation: CONFIG_OPERATIONS.DELETE })),
      },
    ],
    validateOnly,
  });
  return { resource: name, ok: true };
}

export const configUnsetCommand: CommandSpec = {
  path: ['config', 'unset'],
  summary: 'Remove one or more config entries from a resource, reverting them to default',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'type', type: 'string', brief: 'resource type: topic, broker, broker-logger, client-metrics, or group' },
    { name: 'key', type: 'string', multiple: true, brief: 'a config key to remove (repeatable)' },
    { name: 'dry-run', type: 'boolean', brief: 'validate without changing anything' },
  ],
  positionals: [{ name: 'names', variadic: true, brief: 'resource names to update' }],
  examples: [
    'config unset --type topic orders --key retention.ms',
    'config unset --type topic orders payments --key cleanup.policy --dry-run',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('config unset requires at least one resource name');
    }
    const typeFlag = flags.type as string | undefined;
    if (typeFlag === undefined) {
      throw new CliUsageError('config unset requires --type');
    }
    const type = resolveConfigResourceType(typeFlag);

    const keys = flags.key as string[] | undefined;
    if (keys === undefined || keys.length === 0) {
      throw new CliUsageError('config unset requires at least one --key');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const dryRun = flags['dry-run'] === true;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: ResourceResult[];

      if (positionals.length === 1) {
        results = [await unsetOne(admin, type, positionals[0]!, keys, dryRun)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (name) => {
          try {
            return await unsetOne(admin, type, name, keys, dryRun);
          } catch (error) {
            return { resource: name, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['RESOURCE', 'STATUS'],
            results.map((r) => [r.resource, r.ok ? (dryRun ? 'validated' : 'ok') : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
