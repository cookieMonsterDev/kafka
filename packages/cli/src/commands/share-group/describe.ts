import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

interface DescribedShareGroup {
  readonly groupId: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly state?: string;
  readonly epoch?: number;
  readonly assignor?: string;
  readonly memberCount?: number;
}

/**
 * One `describeShareGroups` call per group id: the broker's response throws on the first group
 * with a non-zero error code, discarding every other group's result in the same call — exactly
 * the hazard `group describe` already works around for `describeGroups`.
 */
async function describeOne(admin: Admin, groupId: string): Promise<DescribedShareGroup> {
  const { groups } = await admin.describeShareGroups([groupId]);
  const found = groups[0];
  if (found === undefined) return { groupId, ok: false, detail: 'broker returned no result' };
  return {
    groupId,
    ok: true,
    state: found.groupState,
    epoch: found.groupEpoch,
    assignor: found.assignorName,
    memberCount: found.members.length,
  };
}

function renderHuman(results: readonly DescribedShareGroup[]): string {
  const rows = results.map((result) =>
    result.ok
      ? [
          result.groupId,
          result.state ?? '',
          String(result.epoch ?? ''),
          result.assignor ?? '',
          String(result.memberCount ?? 0),
        ]
      : [result.groupId, '(error)', result.detail ?? 'failed', '', ''],
  );
  return renderTable(['GROUP ID', 'STATE', 'EPOCH', 'ASSIGNOR', 'MEMBERS'], rows);
}

export const shareGroupDescribeCommand: CommandSpec = {
  path: ['share-group', 'describe'],
  summary: 'Describe one or more share groups',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'groupIds', variadic: true, brief: 'share group ids to describe' }],
  examples: ['share-group describe orders-readers --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('share-group describe requires at least one group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: DescribedShareGroup[];

      if (positionals.length === 1) {
        results = [await describeOne(admin, positionals[0]!)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (groupId) => {
          try {
            return await describeOne(admin, groupId);
          } catch (error) {
            return { groupId, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderHuman(results),
        json: () => stringifyJsonSafe({ groups: results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
