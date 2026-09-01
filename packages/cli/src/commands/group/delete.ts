import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

interface GroupResult {
  readonly groupId: string;
  readonly ok: boolean;
  readonly detail?: string;
}

interface DeleteGroupFailure {
  readonly groupId: string;
  readonly errorCode: number;
  readonly error?: { readonly message?: string };
}

/**
 * Matched by `.name`, never `instanceof` — a config file's core might not be the same installed
 * copy as the CLI's own, so a class identity check could silently miss it.
 */
function isKafkaDeleteGroupsError(
  error: unknown,
): error is { readonly name: string; readonly groups: readonly DeleteGroupFailure[] } {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'KafkaDeleteGroupsError' &&
    Array.isArray((error as { groups?: unknown }).groups)
  );
}

export const groupDeleteCommand: CommandSpec = {
  path: ['group', 'delete'],
  summary: 'Delete one or more consumer groups',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
  ],
  positionals: [{ name: 'groupIds', variadic: true, brief: 'group ids to delete' }],
  examples: ['group delete my-group --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('group delete requires at least one group id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const yes = flags.yes === true;

    await confirmDestructive({
      runtime,
      yes,
      message: `Delete group${positionals.length > 1 ? 's' : ''} ${positionals.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: GroupResult[];

      // `deleteGroups` never returns partial results: it resolves with every group's result on
      // full success, or throws `KafkaDeleteGroupsError` — whose `.groups` names only the groups
      // that failed — on any failure. So this is one call for the whole list, not a fan-out.
      try {
        await admin.deleteGroups([...positionals]);
        results = positionals.map((groupId) => ({ groupId, ok: true }));
      } catch (error) {
        if (!isKafkaDeleteGroupsError(error)) throw error;

        const failures = new Map(error.groups.map((failure) => [failure.groupId, failure]));
        results = positionals.map((groupId) => {
          const failure = failures.get(groupId);
          if (failure === undefined) return { groupId, ok: true };
          return { groupId, ok: false, detail: failure.error?.message ?? `failed (code ${failure.errorCode})` };
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['GROUP ID', 'STATUS'],
            results.map((r) => [r.groupId, r.ok ? 'deleted' : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
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
