import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { toAlterQuotaEntity } from './entity';

export const quotaAlterCommand: CommandSpec = {
  path: ['quota', 'alter'],
  summary: 'Set or remove client quota values for one entity',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'entity',
      type: 'string',
      multiple: true,
      keyValue: true,
      brief: 'an entity type=name to alter, or type= for its cluster default (repeatable)',
    },
    { name: 'set', type: 'string', multiple: true, keyValue: true, brief: 'a quota key=value to set (repeatable)' },
    { name: 'unset', type: 'string', multiple: true, brief: 'a quota key to remove (repeatable)' },
    { name: 'dry-run', type: 'boolean', brief: 'validate without changing anything' },
  ],
  examples: [
    'quota alter --entity user=alice --set producer_byte_rate=1048576 --brokers localhost:9092',
    'quota alter --entity user=alice --entity client-id=orders-producer --unset producer_byte_rate --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const entity = toAlterQuotaEntity(flags.entity as Record<string, string> | undefined);

    const setFlags = flags.set as Record<string, string> | undefined;
    const unsetFlags = flags.unset as string[] | undefined;
    const ops = [
      ...Object.entries(setFlags ?? {}).map(([key, value]) => ({
        key,
        value: coerceNumber(value, 'set'),
        remove: false,
      })),
      ...(unsetFlags ?? []).map((key) => ({ key, value: 0, remove: true })),
    ];
    if (ops.length === 0) {
      throw new CliUsageError('quota alter requires at least one --set key=value or --unset key');
    }

    const dryRun = flags['dry-run'] === true;
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      // `alterClientQuotas` targets exactly one entity per call here, and its response resolves
      // only when every op on that entity succeeded — any op error rejects the whole call instead
      // of returning an error entry, so a resolved promise always means success.
      const { entries } = await admin.alterClientQuotas({ entries: [{ entity, ops }], validateOnly: dryRun });

      output.write({
        human: () => (dryRun ? 'validated' : 'ok'),
        json: () => stringifyJsonSafe({ entries }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
