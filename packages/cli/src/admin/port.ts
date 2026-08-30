import type { Admin, AdminConfig } from '@cookiemonsterdev/kafka-core';

/** Everything a command needs to connect to a broker, resolved from `--brokers` and friends. */
export interface OpenAdminOptions {
  brokers: string[];
  clientId?: string;
  config?: AdminConfig;
}

/**
 * Opens a connected {@link Admin} client. A command depends on this port, never on
 * `@cookiemonsterdev/kafka-core` directly, so it can be unit-tested against a fake admin with no
 * broker and no import of core's (relatively large) dist.
 */
export type OpenAdmin = (options: OpenAdminOptions) => Promise<Admin>;
