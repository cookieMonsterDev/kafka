import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const shareGroupDescribeCommand: CommandSpec = {
  path: ['share-group', 'describe'],
  summary: 'Describe one or more share groups',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'groupIds', variadic: true, brief: 'share group ids to describe' }],
  examples: ['share-group describe orders-readers --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('share-group describe requires at least one group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { groups } = await admin.describeShareGroups([...positionals]);

      output.write({
        human: () =>
          renderTable(
            ['GROUP ID', 'STATE', 'EPOCH', 'ASSIGNOR', 'MEMBERS'],
            groups.map((g) => [
              g.groupId,
              g.errorCode === 0 ? g.groupState : `error (code ${String(g.errorCode)})`,
              String(g.groupEpoch),
              g.assignorName,
              String(g.members.length),
            ]),
          ),
        json: () => stringifyJsonSafe({ groups }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
