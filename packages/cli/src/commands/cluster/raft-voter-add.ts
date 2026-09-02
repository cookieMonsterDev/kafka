import { decodeUuid } from '../../admin/decode-args';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';

interface RaftVoterListener {
  readonly name: string;
  readonly host: string;
  readonly port: number;
}

/** Parses one `--listener name=host:port` entry into core's `RaftVoterListener` shape. */
function parseListenerFlag(raw: string): RaftVoterListener {
  const equalsIndex = raw.indexOf('=');
  if (equalsIndex <= 0) {
    throw new CliUsageError(`--listener must be "name=host:port", got "${raw}"`);
  }
  const name = raw.slice(0, equalsIndex);
  const hostPort = raw.slice(equalsIndex + 1);
  const separatorIndex = hostPort.lastIndexOf(':');
  if (separatorIndex <= 0 || separatorIndex === hostPort.length - 1) {
    throw new CliUsageError(`--listener must be "name=host:port", got "${raw}"`);
  }
  const host = hostPort.slice(0, separatorIndex);
  const port = coerceNumber(hostPort.slice(separatorIndex + 1), 'listener');
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new CliUsageError(`--listener port must be between 0 and 65535, got "${raw}"`);
  }
  return { name, host, port };
}

export const clusterRaftVoterAddCommand: CommandSpec = {
  path: ['cluster', 'raft-voter', 'add'],
  summary: 'Add a voter to the KRaft metadata quorum',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'voter-id', type: 'number', brief: 'the new voter’s node id' },
    { name: 'voter-directory-id', type: 'string', brief: 'the new voter’s directory id, as a uuid' },
    {
      name: 'listener',
      type: 'string',
      multiple: true,
      brief: 'a listener the voter is reachable on, name=host:port (repeatable)',
    },
    { name: 'cluster-id', type: 'string', brief: 'expected cluster id, if any' },
    { name: 'timeout-ms', type: 'number', brief: 'request timeout in ms' },
    { name: 'ack-when-committed', type: 'boolean', brief: 'wait for the addition to be committed before returning' },
    { name: 'dry-run', type: 'boolean', brief: 'print the voter that would be added and exit without connecting' },
  ],
  examples: [
    'cluster raft-voter add --voter-id 4 --voter-directory-id 3c48b... --listener CONTROLLER=localhost:9093 --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const voterId = flags['voter-id'] as number | undefined;
    if (voterId === undefined) {
      throw new CliUsageError('cluster raft-voter add requires --voter-id');
    }
    const voterDirectoryIdFlag = flags['voter-directory-id'] as string | undefined;
    if (voterDirectoryIdFlag === undefined) {
      throw new CliUsageError('cluster raft-voter add requires --voter-directory-id');
    }
    const voterDirectoryId = decodeUuid(voterDirectoryIdFlag);

    const listenerFlags = flags.listener as string[] | undefined;
    if (listenerFlags === undefined || listenerFlags.length === 0) {
      throw new CliUsageError('cluster raft-voter add requires at least one --listener name=host:port');
    }
    const listeners = listenerFlags.map(parseListenerFlag);

    const clusterId = flags['cluster-id'] as string | undefined;
    const timeoutMs = flags['timeout-ms'] as number | undefined;
    const ackWhenCommitted = flags['ack-when-committed'] === true;
    const dryRun = flags['dry-run'] === true;

    if (dryRun) {
      output.write({
        human: () =>
          `Would add voter ${String(voterId)} (${voterDirectoryIdFlag}) with listeners: ${listenerFlags.join(', ')}`,
        json: () => stringifyJsonSafe({ voterId, voterDirectoryId, listeners, clusterId }),
      });
      return EXIT_CODES.ok;
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      await admin.addRaftVoter({ clusterId, timeoutMs, voterId, voterDirectoryId, listeners, ackWhenCommitted });

      output.write({
        human: () => `Voter ${String(voterId)} added.`,
        json: () => stringifyJsonSafe({ voterId, ok: true }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
