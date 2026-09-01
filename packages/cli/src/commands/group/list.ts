import type { CommandSpec } from '../../args/define';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const groupListCommand: CommandSpec = {
  path: ['group', 'list'],
  summary: 'List every consumer group the cluster knows about',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['group list --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    let groups: { groupId: string; protocolType: string }[];
    try {
      ({ groups } = await admin.listGroups());
    } finally {
      await admin.disconnect();
    }

    output.write({
      human: () =>
        groups.length === 0
          ? '(no groups)'
          : renderTable(
              ['GROUP ID', 'PROTOCOL TYPE'],
              groups.map((group) => [group.groupId, group.protocolType]),
            ),
      json: () => stringifyJsonSafe({ groups }),
    });
    return EXIT_CODES.ok;
  },
};
