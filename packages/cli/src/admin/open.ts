import type { OpenAdmin } from './port';

/**
 * The real {@link OpenAdmin} implementation. `@cookiemonsterdev/kafka-core` is only imported
 * here, lazily, inside the call — commands that never connect (`--help`, `--version`, usage
 * errors) never pay for loading it.
 */
export const openAdmin: OpenAdmin = async ({ brokers, clientId, config }) => {
  const { Kafka } = await import('@cookiemonsterdev/kafka-core');
  const admin = new Kafka({ brokers, clientId, config: false }).admin(config);
  await admin.connect();
  return admin;
};

export type { OpenAdmin, OpenAdminOptions } from './port';
