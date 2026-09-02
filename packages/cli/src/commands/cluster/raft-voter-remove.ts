import { decodeUuid } from '../../admin/decode-args';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';

export const clusterRaftVoterRemoveCommand: CommandSpec = {
  path: ['cluster', 'raft-voter', 'remove'],
  summary: 'Remove a voter from the KRaft metadata quorum',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'voter-id', type: 'number', brief: 'the voter’s node id' },
    { name: 'voter-directory-id', type: 'string', brief: 'the voter’s directory id, as a uuid' },
    { name: 'cluster-id', type: 'string', brief: 'expected cluster id, if any' },
    { name: 'yes', type: 'boolean', brief: 'confirm the removal without an interactive prompt' },
  ],
  examples: ['cluster raft-voter remove --voter-id 4 --voter-directory-id 3c48b... --brokers localhost:9092 --yes'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const voterId = flags['voter-id'] as number | undefined;
    if (voterId === undefined) {
      throw new CliUsageError('cluster raft-voter remove requires --voter-id');
    }
    const voterDirectoryIdFlag = flags['voter-directory-id'] as string | undefined;
    if (voterDirectoryIdFlag === undefined) {
      throw new CliUsageError('cluster raft-voter remove requires --voter-directory-id');
    }
    const voterDirectoryId = decodeUuid(voterDirectoryIdFlag);
    const clusterId = flags['cluster-id'] as string | undefined;

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Remove voter ${String(voterId)} from the quorum?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      await admin.removeRaftVoter({ clusterId, voterId, voterDirectoryId });

      output.write({
        human: () => `Voter ${String(voterId)} removed.`,
        json: () => stringifyJsonSafe({ voterId, ok: true }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
