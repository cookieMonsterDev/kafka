import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { toDescribeQuotaComponents } from './entity';

function formatEntity(entity: readonly { entityType: string; entityName: string | null }[]): string {
  if (entity.length === 0) return '(cluster default)';
  return entity.map((e) => `${e.entityType}=${e.entityName ?? '(default)'}`).join(', ');
}

export const quotaDescribeCommand: CommandSpec = {
  path: ['quota', 'describe'],
  summary: 'Describe client quotas matching an entity filter',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'entity',
      type: 'string',
      multiple: true,
      keyValue: true,
      brief: 'an entity type=name to match exactly, or type= for its cluster default (repeatable)',
    },
    {
      name: 'entity-any',
      type: 'string',
      multiple: true,
      brief: 'an entity type to match with any specified name (repeatable)',
    },
    { name: 'strict', type: 'boolean', brief: 'reject entity types with no filter component' },
  ],
  examples: [
    'quota describe --entity user=alice --brokers localhost:9092',
    'quota describe --entity-any client-id --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const components = toDescribeQuotaComponents(
      flags.entity as Record<string, string> | undefined,
      flags['entity-any'] as string[] | undefined,
    );
    const strict = flags.strict === true;

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { entries } = await admin.describeClientQuotas({ components, strict });

      const rows = entries.flatMap((entry) =>
        entry.values.length === 0
          ? [[formatEntity(entry.entity), '(none)', '-']]
          : entry.values.map((value) => [formatEntity(entry.entity), value.key, String(value.value)]),
      );

      output.write({
        human: () => (rows.length === 0 ? '(no matching quotas)' : renderTable(['ENTITY', 'KEY', 'VALUE'], rows)),
        json: () => stringifyJsonSafe({ entries }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
