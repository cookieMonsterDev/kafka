import type { Admin, AdminConfig } from '@cookiemonsterdev/kafka-core';
import type { ResolvedCliConfig } from '../config/resolve';

/**
 * Everything a command needs to connect to a broker. `brokers`/`clientId` come from the command's
 * own flags — both optional, since a config file, an environment variable, or a `--profile` may
 * supply them instead; `env`/`config` are what the real implementation merges those from (see
 * `config/connection.ts`), threaded through here rather than read from `process.env` directly so
 * this stays a fake-able port.
 */
export interface OpenAdminOptions {
  readonly brokers?: readonly string[];
  readonly clientId?: string;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly config: ResolvedCliConfig;
  readonly adminConfig?: AdminConfig;
}

/**
 * Opens a connected {@link Admin} client. A command depends on this port, never on
 * `@cookiemonsterdev/kafka-core` directly, so it can be unit-tested against a fake admin with no
 * broker and no import of core's (relatively large) dist.
 */
export type OpenAdmin = (options: OpenAdminOptions) => Promise<Admin>;
