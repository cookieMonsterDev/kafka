import { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';
import type { ResolvedCliConfig } from './resolve';

const SHALLOW_MERGE_KEYS = ['retry'];

export interface ConnectionFlagOverrides {
  readonly brokers?: readonly string[];
  readonly clientId?: string;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function clientSectionOf(fileConfig: Record<string, unknown> | null): Record<string, unknown> | undefined {
  const client = fileConfig?.client;
  return isPlainObject(client) ? client : undefined;
}

/**
 * Merges every layer the CLI itself is responsible for, highest first: the command's own flags,
 * environment variables (`fromEnv` — core-only, so the caller computes it and hands it in rather
 * than this module importing core), and the active `--profile`. `Kafka.from(fileConfig, overrides)`
 * merges the result of this function over the config file's `client` section and the constructor's
 * own defaults, so the full precedence ends up flags → env → profile → file → default.
 *
 * `cli.timeoutMs` is the one exception: a CLI-level default of last resort, applied only when
 * nothing above it — not even the config file's own `client` section — set a timeout.
 */
export function buildConnectionOverrides(
  flags: ConnectionFlagOverrides,
  envOverrides: Record<string, unknown>,
  config: ResolvedCliConfig,
): Record<string, unknown> {
  const profile = config.profile !== null ? (config.cli.profiles?.[config.profile] ?? {}) : {};
  const flagLayer: Record<string, unknown> = {};
  if (flags.brokers !== undefined) flagLayer.brokers = flags.brokers;
  if (flags.clientId !== undefined) flagLayer.clientId = flags.clientId;

  const flagsOverEnv = mergeConfigLayers(flagLayer, envOverrides, { shallowMergeKeys: SHALLOW_MERGE_KEYS });
  const overrides = mergeConfigLayers(flagsOverEnv, profile as Record<string, unknown>, {
    shallowMergeKeys: SHALLOW_MERGE_KEYS,
  });

  const timeoutMs = config.cli.timeoutMs;
  if (timeoutMs !== undefined) {
    const client = clientSectionOf(config.fileConfig);
    if (overrides.connectionTimeout === undefined && client?.connectionTimeout === undefined) {
      overrides.connectionTimeout = timeoutMs;
    }
    if (overrides.requestTimeout === undefined && client?.requestTimeout === undefined) {
      overrides.requestTimeout = timeoutMs;
    }
  }

  return overrides;
}
