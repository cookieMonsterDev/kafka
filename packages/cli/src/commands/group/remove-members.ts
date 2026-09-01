import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

interface MemberInput {
  readonly memberId: string;
  readonly groupInstanceId?: string;
}

interface MemberResult {
  readonly memberId: string;
  readonly groupInstanceId: string | null;
  readonly errorCode: number;
  readonly ok: boolean;
}

/** `memberId` or `memberId:groupInstanceId`, split on the first colon. */
function parseMember(raw: string): MemberInput {
  const index = raw.indexOf(':');
  if (index === -1) return { memberId: raw };
  return { memberId: raw.slice(0, index), groupInstanceId: raw.slice(index + 1) };
}

export const groupRemoveMembersCommand: CommandSpec = {
  path: ['group', 'remove-members'],
  summary: "Remove one or more static members from a consumer group's session",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'yes', type: 'boolean', brief: 'confirm the removal without an interactive prompt' },
    {
      name: 'member',
      type: 'string',
      multiple: true,
      brief: 'member to remove, memberId or memberId:groupInstanceId (repeatable; at least one required)',
    },
  ],
  positionals: [{ name: 'groupId', brief: 'group id' }],
  examples: ['group remove-members my-group --member consumer-1-abc --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, positionals, runtime, output, config }) {
    const groupId = positionals[0];
    if (groupId === undefined) {
      throw new CliUsageError('group remove-members requires a group id');
    }

    const rawMembers = flags.member as string[] | undefined;
    if (rawMembers === undefined || rawMembers.length === 0) {
      throw new CliUsageError('group remove-members requires at least one --member');
    }
    const members = rawMembers.map(parseMember);

    const brokers = parseBrokersFlag(flags.brokers);
    const yes = flags.yes === true;

    await confirmDestructive({
      runtime,
      yes,
      message: `Remove ${members.length > 1 ? `${String(members.length)} members` : 'member'} from group "${groupId}"?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      // `removeMembersFromConsumerGroup` never throws for a bad member — every result carries its
      // own `errorCode` (0 = success), so this is a single call with a per-member exit-code check.
      const { members: responseMembers } = await admin.removeMembersFromConsumerGroup({ groupId, members });
      const results: MemberResult[] = responseMembers.map((member) => ({
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId,
        errorCode: member.errorCode,
        ok: member.errorCode === 0,
      }));

      output.write({
        human: () =>
          renderTable(
            ['MEMBER ID', 'GROUP INSTANCE ID', 'STATUS'],
            results.map((r) => [
              r.memberId,
              r.groupInstanceId ?? '',
              r.ok ? 'ok' : `failed (code ${String(r.errorCode)})`,
            ]),
          ),
        json: () => stringifyJsonSafe({ members: results }),
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
