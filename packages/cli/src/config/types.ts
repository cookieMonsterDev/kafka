/** Defaults applied to `topic create` whenever the equivalent flag is omitted. */
export interface CliTopicDefaults {
  readonly partitions?: number;
  readonly replicationFactor?: number;
}

/**
 * A named, alternate connection layer selected via `--profile`/`KAFKA_PROFILE` — e.g. one entry
 * per cluster (`staging`, `production`). Sits between environment variables and the config file's
 * own `client` section in the merge order (see `config/connection.ts`).
 */
export interface CliProfile {
  readonly brokers?: readonly string[];
  readonly clientId?: string;
  readonly [key: string]: unknown;
}

/**
 * The `cli:` section of a `kafka.config.*` file — this package's own extension of the shared,
 * forward-compatible config shape. An older CLI never rejects a file carrying a section it
 * doesn't know about yet, and this CLI never rejects a key inside `cli:` it doesn't recognize
 * either — both warn, never throw.
 */
export interface CliFileConfig {
  readonly output?: 'human' | 'json';
  /**
   * Reserved for an upcoming destructive-operation confirmation feature (a `--yes`/`--force`
   * layer for commands like `topic delete`, none of which exist on this CLI yet). Parsed and
   * round-tripped today so a config file written against that feature already validates; nothing
   * reads it yet.
   */
  readonly confirmDestructive?: boolean;
  readonly timeoutMs?: number;
  readonly topicDefaults?: CliTopicDefaults;
  readonly profiles?: Readonly<Record<string, CliProfile>>;
}
