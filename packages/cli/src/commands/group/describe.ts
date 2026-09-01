import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';

const CONCURRENCY = 8;

interface DescribedMember {
  readonly memberId: string;
  readonly clientId: string;
  readonly clientHost: string;
}

interface DescribedGroup {
  readonly groupId: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly state?: string;
  readonly protocolType?: string;
  readonly protocol?: string;
  readonly members?: readonly DescribedMember[];
}

/**
 * One `describeGroups` call per group id: the broker's response throws on the first group with a
 * non-zero error code, discarding every other group's result in the same call — exactly the
 * hazard `config describe` already works around for `describeConfigs`.
 */
async function describeOne(admin: Admin, groupId: string): Promise<DescribedGroup> {
  const { groups } = await admin.describeGroups([groupId]);
  const found = groups[0];
  if (found === undefined) return { groupId, ok: false, detail: 'broker returned no result' };

  // A group id the coordinator has never seen (or has fully forgotten) isn't reported as an
  // error — the broker replies with `errorCode: 0` and `state: "Dead"` — so this has to be
  // checked explicitly, or `group describe nonexistent-group` would exit 0.
  if (found.state === 'Dead') return { groupId, ok: false, detail: 'group does not exist' };

  return {
    groupId,
    ok: true,
    state: found.state,
    protocolType: found.protocolType,
    protocol: found.protocol,
    // The raw `memberMetadata`/`memberAssignment` buffers aren't meaningfully renderable and
    // would bloat JSON output, so only the identifying fields survive here.
    members: found.members.map((member) => ({
      memberId: member.memberId,
      clientId: member.clientId,
      clientHost: member.clientHost,
    })),
  };
}

function renderHuman(results: readonly DescribedGroup[]): string {
  const rows = results.map((result) =>
    result.ok
      ? [
          result.groupId,
          result.state ?? '',
          result.protocolType ?? '',
          result.protocol ?? '',
          String(result.members?.length ?? 0),
        ]
      : [result.groupId, '(error)', result.detail ?? 'failed', '', ''],
  );
  return renderTable(['GROUP ID', 'STATE', 'PROTOCOL TYPE', 'PROTOCOL', 'MEMBERS'], rows);
}

export const groupDescribeCommand: CommandSpec = {
  path: ['group', 'describe'],
  summary: 'Describe one or more consumer groups',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'groupIds', variadic: true, brief: 'group ids to describe' }],
  examples: ['group describe my-group --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('group describe requires at least one group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: DescribedGroup[];

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

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
