import type { KafkaConfig } from '../types/index';
import type { KafkaFileConfig } from './types';

/** Which layer supplied a `KafkaConfig` key's value: the call site, a `kafka.config.*` file, or neither. */
export type ConfigKeySource = 'explicit' | 'file' | 'default';

/**
 * Reported by {@link import('../client').Kafka.configSource}. Carries provenance only — never a
 * value — so there is nothing here to redact regardless of which key it names.
 */
export interface KafkaConfigSource {
  /** Absolute path of the config file that was used, or `null` if none was. */
  path: string | null;
  keys: Readonly<Record<KafkaConfigKey, ConfigKeySource>>;
}

/**
 * Every key of `KafkaConfig` (besides `config` itself, which is a resolution instruction, not a
 * resolved value) must appear here. A key added to `KafkaConfig` later without a corresponding
 * entry fails `tsc`, not a missed test.
 */
const CONFIG_KEY_SENTINELS: { [K in keyof Required<Omit<KafkaConfig, 'config'>>]: true } = {
  brokers: true,
  ssl: true,
  sasl: true,
  clientId: true,
  connectionTimeout: true,
  connectionsMaxIdleMs: true,
  socketConnectionSetupTimeoutMaxMs: true,
  clientDnsLookup: true,
  reconnectBackoffMs: true,
  reconnectBackoffMaxMs: true,
  authenticationTimeout: true,
  reauthenticationThreshold: true,
  requestTimeout: true,
  enforceRequestTimeout: true,
  metadataRecovery: true,
  retry: true,
  socketFactory: true,
  logLevel: true,
  logCreator: true,
  metrics: true,
  enableMetricsPush: true,
};

export type KafkaConfigKey = keyof typeof CONFIG_KEY_SENTINELS;

const CONFIG_KEYS = Object.keys(CONFIG_KEY_SENTINELS) as readonly KafkaConfigKey[];

/** Builds the per-key provenance report backing `kafka.configSource()`. */
export function buildKafkaConfigSource(
  explicit: KafkaConfig,
  fileConfig: KafkaFileConfig | null,
  path: string | null,
): KafkaConfigSource {
  const client = fileConfig?.client;
  const keys = {} as Record<KafkaConfigKey, ConfigKeySource>;

  for (const key of CONFIG_KEYS) {
    if (explicit[key] !== undefined) {
      keys[key] = 'explicit';
    } else if (client?.[key] !== undefined) {
      keys[key] = 'file';
    } else {
      keys[key] = 'default';
    }
  }

  return { path, keys };
}
