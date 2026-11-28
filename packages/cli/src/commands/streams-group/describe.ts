import type { Admin, StreamsTopologyDescription } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { isGroupIdNotFoundError } from '../../admin/protocol-error';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { describeCode, formatCode, STREAMS_TOPOLOGY_DESCRIPTION_STATUSES } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

interface DescribedStreamsGroup {
  readonly groupId: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly state?: string;
  readonly epoch?: number;
  readonly topologyEpoch?: number;
  readonly assignor?: string | null;
  readonly memberCount?: number;
  readonly topologyDescriptionStatus?: string;
  readonly topologyDescription?: StreamsTopologyDescription | null;
}

/**
 * One `describeStreamsGroups` call per group id: the broker's response throws on the first group
 * with a non-zero error code, discarding every other group's result in the same call, the same
 * hazard `share-group describe` works around for `describeShareGroups`.
 */
async function describeOne(
  admin: Admin,
  groupId: string,
  includeTopologyDescription: boolean,
): Promise<DescribedStreamsGroup> {
  let groups: Awaited<ReturnType<Admin['describeStreamsGroups']>>['groups'];
  try {
    ({ groups } = await admin.describeStreamsGroups([groupId], { includeTopologyDescription }));
  } catch (error) {
    // The broker's message says whether the group is unknown or is not a streams group.
    if (isGroupIdNotFoundError(error)) {
      return { groupId, ok: false, detail: error instanceof Error ? error.message : 'group does not exist' };
    }
    throw error;
  }
  const found = groups[0];
  if (found === undefined) return { groupId, ok: false, detail: 'broker returned no result' };
  const described: DescribedStreamsGroup = {
    groupId,
    ok: true,
    state: found.groupState,
    epoch: found.groupEpoch,
    topologyEpoch: found.topology?.epoch,
    assignor: found.assignorName,
    memberCount: found.members.length,
  };
  if (!includeTopologyDescription) return described;
  return {
    ...described,
    topologyDescriptionStatus: formatCode(
      describeCode(STREAMS_TOPOLOGY_DESCRIPTION_STATUSES, found.topologyDescriptionStatus),
    ),
    topologyDescription: found.topologyDescription,
  };
}

function renderHuman(results: readonly DescribedStreamsGroup[], includeTopologyDescription: boolean): string {
  const headers = ['GROUP ID', 'STATE', 'EPOCH', 'TOPOLOGY EPOCH', 'ASSIGNOR', 'MEMBERS'];
  if (includeTopologyDescription) headers.push('TOPOLOGY DESCRIPTION');
  const rows = results.map((result) => {
    if (!result.ok) {
      const emptyCells = Array<string>(headers.length - 3).fill('');
      return [result.groupId, '(error)', result.detail ?? 'failed', ...emptyCells];
    }
    const row = [
      result.groupId,
      result.state ?? '',
      String(result.epoch ?? ''),
      String(result.topologyEpoch ?? ''),
      result.assignor ?? '',
      String(result.memberCount ?? 0),
    ];
    if (includeTopologyDescription) row.push(result.topologyDescriptionStatus ?? '');
    return row;
  });
  return renderTable(headers, rows);
}

export const streamsGroupDescribeCommand: CommandSpec = {
  path: ['streams-group', 'describe'],
  summary: 'Describe one or more streams groups',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'topology-description',
      type: 'boolean',
      brief: "also read each group's topology description (needs Kafka 4.4+)",
    },
  ],
  positionals: [{ name: 'groupIds', variadic: true, brief: 'streams group ids to describe' }],
  examples: [
    'streams-group describe word-count --brokers localhost:9092',
    'streams-group describe word-count --topology-description --json',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('streams-group describe requires at least one group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const includeTopologyDescription = flags['topology-description'] === true;
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: DescribedStreamsGroup[];

      if (positionals.length === 1) {
        results = [await describeOne(admin, positionals[0]!, includeTopologyDescription)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (groupId) => {
          try {
            return await describeOne(admin, groupId, includeTopologyDescription);
          } catch (error) {
            return { groupId, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderHuman(results, includeTopologyDescription),
        json: () => stringifyJsonSafe({ groups: results }),
      });

      return exitForBatchResults(results, (result) => result.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
