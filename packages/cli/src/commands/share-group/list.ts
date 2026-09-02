import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

/** KIP-932's protocol type for a share group, as `listGroups` reports it alongside `'consumer'`. */
const SHARE_GROUP_PROTOCOL_TYPE = 'share';

export const shareGroupListCommand: CommandSpec = {
  path: ['share-group', 'list'],
  summary: 'List every share group the cluster knows about',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['share-group list --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { groups: allGroups } = await admin.listGroups();
      const groups = allGroups.filter((group) => group.protocolType === SHARE_GROUP_PROTOCOL_TYPE);

      output.write({
        human: () =>
          groups.length === 0
            ? '(no share groups)'
            : renderTable(
                ['GROUP ID'],
                groups.map((g) => [g.groupId]),
              ),
        json: () => stringifyJsonSafe({ groups }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
