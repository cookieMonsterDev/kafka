import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive, requireForce } from '../../interaction/confirm';
import { FEATURE_UPDATE_UPGRADE_TYPES } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { resolveFeatureUpgradeType } from './enums';

interface FeatureResult {
  readonly feature: string;
  readonly ok: boolean;
  readonly detail?: string;
}

interface UpdateFeatureFailure {
  readonly feature: string;
  readonly message: string;
}

/**
 * Matched by `.name`, never `instanceof` — a config file's core might not be the same installed
 * copy as the CLI's own, so a class identity check could silently miss it. Mirrors
 * `group/delete.ts`'s `isKafkaDeleteGroupsError`: `updateFeatures` either resolves on full success
 * or throws one `KafkaAggregateError` wrapping a `KafkaUpdateFeaturesError` per failed feature —
 * verified against `packages/core/src/protocol/requests/update-features/v0/response.ts`.
 */
function isKafkaAggregateError(
  error: unknown,
): error is { readonly name: string; readonly errors: readonly unknown[] } {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'KafkaAggregateError' &&
    Array.isArray((error as { errors?: unknown }).errors)
  );
}

function isUpdateFeatureError(item: unknown): item is UpdateFeatureFailure {
  return (
    typeof item === 'object' &&
    item !== null &&
    (item as { name?: unknown }).name === 'KafkaUpdateFeaturesError' &&
    typeof (item as { feature?: unknown }).feature === 'string'
  );
}

export const clusterUpdateFeaturesCommand: CommandSpec = {
  path: ['cluster', 'update-features'],
  summary: 'Upgrade, safe-downgrade, or unsafe-downgrade one or more finalized feature versions',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'feature',
      type: 'string',
      multiple: true,
      keyValue: true,
      brief: 'a feature to update, name=maxVersionLevel (repeatable)',
    },
    {
      name: 'upgrade-type',
      type: 'string',
      brief: 'upgrade, safe-downgrade, or unsafe-downgrade (default: upgrade), applied to every --feature',
    },
    { name: 'timeout', type: 'number', brief: 'request timeout in ms' },
    { name: 'dry-run', type: 'boolean', brief: 'validate on the broker without finalizing anything' },
    { name: 'yes', type: 'boolean', brief: 'confirm the update without an interactive prompt' },
    { name: 'force', type: 'boolean', brief: 'required in addition to --yes for an unsafe downgrade' },
  ],
  examples: [
    'cluster update-features --feature kraft.version=1 --brokers localhost:9092 --yes',
    'cluster update-features --feature kraft.version=0 --upgrade-type unsafe-downgrade --yes --force --brokers localhost:9092',
  ],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, runtime, output, config }) {
    const featureFlags = flags.feature as Record<string, string> | undefined;
    if (featureFlags === undefined || Object.keys(featureFlags).length === 0) {
      throw new CliUsageError('cluster update-features requires at least one --feature name=maxVersionLevel');
    }

    const upgradeType = resolveFeatureUpgradeType((flags['upgrade-type'] as string | undefined) ?? 'upgrade');
    const featureNames = Object.keys(featureFlags);
    const featureUpdates = featureNames.map((feature) => ({
      feature,
      maxVersionLevel: coerceNumber(featureFlags[feature]!, 'feature'),
      upgradeType,
    }));

    const timeout = flags.timeout as number | undefined;
    const dryRun = flags['dry-run'] === true;
    const brokers = parseBrokersFlag(flags.brokers);

    if (!dryRun) {
      if (upgradeType === FEATURE_UPDATE_UPGRADE_TYPES.UNSAFE_DOWNGRADE) {
        requireForce({
          force: flags.force === true,
          reason: `unsafe-downgrading feature${featureNames.length > 1 ? 's' : ''} ${featureNames.join(', ')}`,
        });
      }
      await confirmDestructive({
        runtime,
        yes: flags.yes === true,
        message: `Update feature${featureNames.length > 1 ? 's' : ''} ${featureNames.join(', ')}?`,
        confirmDestructive: config.cli.confirmDestructive,
      });
    }

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: FeatureResult[];

      try {
        await admin.updateFeatures({ featureUpdates, timeout, validateOnly: dryRun });
        results = featureNames.map((feature) => ({ feature, ok: true }));
      } catch (error) {
        if (!isKafkaAggregateError(error)) throw error;

        const failures = new Map(
          error.errors.filter(isUpdateFeatureError).map((failure) => [failure.feature, failure]),
        );
        results = featureNames.map((feature) => {
          const failure = failures.get(feature);
          if (failure === undefined) return { feature, ok: true };
          return { feature, ok: false, detail: failure.message };
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['FEATURE', 'STATUS'],
            results.map((r) => [r.feature, r.ok ? (dryRun ? 'validated' : 'updated') : (r.detail ?? 'failed')]),
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
