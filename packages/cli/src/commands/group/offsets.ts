import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const groupOffsetsCommand: CommandSpec = {
  path: ['group', 'offsets'],
  summary: "Show a consumer group's committed offsets",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'topic',
      type: 'string',
      multiple: true,
      brief: 'limit the result to this topic (repeatable; defaults to every topic the group has offsets for)',
    },
  ],
  positionals: [{ name: 'groupId', brief: 'group id' }],
  examples: ['group offsets my-group --brokers localhost:9092', 'group offsets my-group --topic orders'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    const groupId = positionals[0];
    if (groupId === undefined) {
      throw new CliUsageError('group offsets requires a group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const topics = flags.topic as string[] | undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      // `resolveOffsets: true` isn't just a read on this API — it makes `fetchOffsets` commit the
      // resolved offset back to the broker via `setOffsets`. `group offsets` is a read command, so
      // this is deliberately always `false`.
      const results = await admin.fetchOffsets({ groupId, topics, resolveOffsets: false });

      const rows = results.flatMap((result) =>
        result.partitions.map((partition) => [
          result.topic,
          String(partition.partition),
          partition.offset.toString(),
          partition.metadata ?? '',
        ]),
      );

      output.write({
        human: () =>
          rows.length === 0 ? '(no offsets)' : renderTable(['TOPIC', 'PARTITION', 'OFFSET', 'METADATA'], rows),
        json: () => stringifyJsonSafe(results),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
