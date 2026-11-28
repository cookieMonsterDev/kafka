import type { ListedGroup } from '@cookiemonsterdev/kafka-core';
import type { CommandSpec } from '../../args/define';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

/** The group types a ListGroups v5 broker can filter by (KIP-848). */
const GROUP_TYPES = ['classic', 'consumer', 'share', 'streams'];

export const groupListCommand: CommandSpec = {
  path: ['group', 'list'],
  summary: 'List the groups the cluster knows about, optionally by group type',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'type',
      type: 'enum',
      multiple: true,
      values: GROUP_TYPES,
      brief: 'limit to groups of this type (repeatable; default: all; needs Kafka 3.8+)',
    },
  ],
  examples: ['group list --brokers localhost:9092', 'group list --type consumer --type classic'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    // A repeatable flag that was not passed parses as `[]`, which core reads as "no filter".
    const types = flags.type as string[] | undefined;
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    let groups: ListedGroup[];
    try {
      ({ groups } = await admin.listGroups({ types }));
    } finally {
      await admin.disconnect();
    }

    output.write({
      human: () =>
        groups.length === 0
          ? '(no groups)'
          : renderTable(
              ['GROUP ID', 'PROTOCOL TYPE', 'GROUP TYPE'],
              groups.map((group) => [group.groupId, group.protocolType, group.groupType ?? '']),
            ),
      json: () => stringifyJsonSafe({ groups }),
    });
    return EXIT_CODES.ok;
  },
};
