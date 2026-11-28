import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

/** KIP-1071's protocol type for a streams group, as `listGroups` reports it alongside `'consumer'`. */
const STREAMS_GROUP_PROTOCOL_TYPE = 'streams';

export const streamsGroupListCommand: CommandSpec = {
  path: ['streams-group', 'list'],
  summary: 'List every streams group the cluster knows about',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['streams-group list --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { groups: allGroups } = await admin.listGroups();
      const groups = allGroups.filter((group) => group.protocolType === STREAMS_GROUP_PROTOCOL_TYPE);

      output.write({
        human: () =>
          groups.length === 0
            ? '(no streams groups)'
            : renderTable(
                ['GROUP ID', 'STATE'],
                groups.map((group) => [group.groupId, group.groupState ?? '']),
              ),
        json: () => stringifyJsonSafe({ groups }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
