import type { KafkaConfig } from '@cookiemonsterdev/kafka-core';
import { buildConnectionOverrides } from '../config/connection';
import type { OpenAdmin } from './port';

/**
 * The real {@link OpenAdmin} implementation. `@cookiemonsterdev/kafka-core` is only imported
 * here, lazily, inside the call — commands that never connect (`--help`, `--version`, usage
 * errors, `init`, `profiles`) never pay for loading it. `Kafka.from()` — not `new Kafka()` — is
 * used deliberately: the CLI already resolved (and, if needed, discovered) the config file itself
 * in `main()`, so a second, redundant discovery inside the constructor would both waste a
 * filesystem read and corrupt `configSource()`'s provenance.
 */
export const openAdmin: OpenAdmin = async ({ brokers, clientId, env, config, adminConfig }) => {
  const { Kafka, fromEnv } = await import('@cookiemonsterdev/kafka-core');
  const envOverrides = fromEnv(env);
  const overrides = buildConnectionOverrides({ brokers, clientId }, envOverrides, config) as KafkaConfig;
  const kafka = Kafka.from(config.fileConfig ?? {}, overrides);
  const admin = kafka.admin(adminConfig);
  await admin.connect();
  return admin;
};

export type { OpenAdmin, OpenAdminOptions } from './port';
