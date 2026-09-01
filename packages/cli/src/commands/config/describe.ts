import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { REDACTED } from '../../admin/redact';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { CONFIG_SOURCE, describeCode, formatCode } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';
import { resolveConfigResourceType, type ConfigResourceType } from './resource-type';

const CONCURRENCY = 8;

interface DescribedEntry {
  readonly name: string;
  readonly value: string | null;
  readonly isDefault: boolean;
  readonly isSensitive: boolean;
  readonly source: string;
}

interface DescribedResource {
  readonly resource: string;
  readonly ok: boolean;
  readonly entries?: readonly DescribedEntry[];
  readonly detail?: string;
}

async function describeOne(
  admin: Admin,
  type: ConfigResourceType,
  name: string,
  configNames: readonly string[] | undefined,
  includeSynonyms: boolean,
  includeDocumentation: boolean,
  showSecrets: boolean,
): Promise<DescribedResource> {
  const { resources } = await admin.describeConfigs({
    resources: [{ type, name, configNames: configNames !== undefined ? [...configNames] : undefined }],
    includeSynonyms,
    includeDocumentation,
  });
  const found = resources[0];
  if (found === undefined) return { resource: name, ok: false, detail: 'broker returned no result' };

  return {
    resource: name,
    ok: true,
    entries: found.configEntries.map((entry) => ({
      name: entry.configName,
      // The broker already nulls a sensitive value it doesn't want to disclose, but that's its
      // policy, not this CLI's contract — an entry marked `isSensitive` gets redacted here too,
      // the same belt-and-suspenders `--show-secrets` opt-out `admin call` already applies.
      value: entry.isSensitive && !showSecrets ? REDACTED : entry.configValue,
      isDefault: entry.isDefault,
      isSensitive: entry.isSensitive,
      source: formatCode(describeCode(CONFIG_SOURCE, entry.configSource)),
    })),
  };
}

function renderHuman(resources: readonly DescribedResource[]): string {
  const rows: string[][] = [];
  for (const resource of resources) {
    if (!resource.ok) {
      rows.push([resource.resource, '(error)', resource.detail ?? 'failed', '', '', '']);
      continue;
    }
    for (const entry of resource.entries ?? []) {
      rows.push([
        resource.resource,
        entry.name,
        entry.value ?? '(null)',
        entry.source,
        String(entry.isDefault),
        String(entry.isSensitive),
      ]);
    }
  }
  if (rows.length === 0) return '(no config entries)';
  return renderTable(['RESOURCE', 'CONFIG', 'VALUE', 'SOURCE', 'DEFAULT', 'SENSITIVE'], rows);
}

export const configDescribeCommand: CommandSpec = {
  path: ['config', 'describe'],
  summary: 'Describe the configs of one or more resources',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'type', type: 'string', brief: 'resource type: topic, broker, broker-logger, client-metrics, or group' },
    {
      name: 'config-name',
      type: 'string',
      multiple: true,
      brief: 'limit the result to this config key (repeatable, default: every key)',
    },
    { name: 'include-synonyms', type: 'boolean', brief: "include each entry's config synonyms" },
    { name: 'include-documentation', type: 'boolean', brief: "include each entry's documentation string" },
    {
      name: 'show-secrets',
      type: 'boolean',
      brief: 'print a sensitive config value instead of redacting it',
    },
  ],
  positionals: [{ name: 'names', variadic: true, brief: 'resource names to describe' }],
  examples: [
    'config describe --type topic orders --brokers localhost:9092',
    'config describe --type broker 1 --include-synonyms --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('config describe requires at least one resource name');
    }
    const typeFlag = flags.type as string | undefined;
    if (typeFlag === undefined) {
      throw new CliUsageError('config describe requires --type');
    }
    const type = resolveConfigResourceType(typeFlag);

    const brokers = parseBrokersFlag(flags.brokers);
    const configNames = flags['config-name'] as string[] | undefined;
    const includeSynonyms = flags['include-synonyms'] === true;
    const includeDocumentation = flags['include-documentation'] === true;
    const showSecrets = flags['show-secrets'] === true;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: DescribedResource[];

      if (positionals.length === 1) {
        results = [
          await describeOne(
            admin,
            type,
            positionals[0]!,
            configNames,
            includeSynonyms,
            includeDocumentation,
            showSecrets,
          ),
        ];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (name) => {
          try {
            return await describeOne(
              admin,
              type,
              name,
              configNames,
              includeSynonyms,
              includeDocumentation,
              showSecrets,
            );
          } catch (error) {
            return { resource: name, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderHuman(results),
        json: () => stringifyJsonSafe({ resources: results }),
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
