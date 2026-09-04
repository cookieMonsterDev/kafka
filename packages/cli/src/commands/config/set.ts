import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { CONFIG_OPERATIONS } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';
import { resolveConfigResourceType, type ConfigResourceType } from './resource-type';

const CONCURRENCY = 8;

interface ResourceResult {
  readonly resource: string;
  readonly ok: boolean;
  readonly detail?: string;
}

async function setOne(
  admin: Admin,
  type: ConfigResourceType,
  name: string,
  entries: Readonly<Record<string, string>>,
  validateOnly: boolean,
): Promise<ResourceResult> {
  await admin.incrementalAlterConfigs({
    resources: [
      {
        type,
        name,
        configs: Object.entries(entries).map(([configName, value]) => ({
          name: configName,
          value,
          operation: CONFIG_OPERATIONS.SET,
        })),
      },
    ],
    validateOnly,
  });
  return { resource: name, ok: true };
}

export const configSetCommand: CommandSpec = {
  path: ['config', 'set'],
  summary: 'Set one or more config entries on a resource',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'type', type: 'string', brief: 'resource type: topic, broker, broker-logger, client-metrics, or group' },
    {
      name: 'entry',
      type: 'string',
      multiple: true,
      keyValue: true,
      brief: 'a config entry to set, key=value (repeatable)',
    },
    { name: 'dry-run', type: 'boolean', brief: 'validate without changing anything' },
  ],
  positionals: [{ name: 'names', variadic: true, brief: 'resource names to update' }],
  examples: [
    'config set --type topic orders --entry retention.ms=604800000',
    'config set --type topic orders payments --entry cleanup.policy=compact --dry-run',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('config set requires at least one resource name');
    }
    const typeFlag = flags.type as string | undefined;
    if (typeFlag === undefined) {
      throw new CliUsageError('config set requires --type');
    }
    const type = resolveConfigResourceType(typeFlag);

    const entries = flags.entry as Record<string, string> | undefined;
    if (entries === undefined || Object.keys(entries).length === 0) {
      throw new CliUsageError('config set requires at least one --entry key=value');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const dryRun = flags['dry-run'] === true;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: ResourceResult[];

      if (positionals.length === 1) {
        results = [await setOne(admin, type, positionals[0]!, entries, dryRun)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (name) => {
          try {
            return await setOne(admin, type, name, entries, dryRun);
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

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
