import type { Admin } from '@cookiemonsterdev/kafka-core';

export type PooledAdmin = Pick<
  Admin,
  | 'connect'
  | 'disconnect'
  | 'listTopics'
  | 'fetchTopicMetadata'
  | 'describeTopicPartitions'
  | 'fetchTopicOffsets'
  | 'fetchTopicOffsetsByTimestamp'
  | 'describeConfigs'
  | 'describeLogDirs'
  | 'createTopics'
  | 'deleteTopics'
  | 'deleteTopicRecords'
  | 'createPartitions'
  | 'incrementalAlterConfigs'
  | 'listGroups'
  | 'describeGroups'
  | 'fetchOffsets'
  | 'setOffsets'
  | 'resetOffsets'
  | 'deleteGroups'
  | 'deleteGroupOffsets'
  | 'removeMembersFromConsumerGroup'
  | 'describeShareGroups'
  | 'listShareGroupOffsets'
>;

/** Everything `AdminPool` needs to build a fresh, unconnected client for one profile. */
export type KafkaClientFactory = (profileName: string | null) => { admin(): PooledAdmin };

const DEFAULT_KEY = '__default__';

function keyFor(profileName: string | null): string {
  return profileName ?? DEFAULT_KEY;
}

/**
 * Caches one connected admin per profile so switching views in the UI doesn't pay a fresh TCP
 * handshake and metadata fetch on every request — only the first request for a profile the
 * studio hasn't talked to yet, or one explicitly {@link invalidate}d, reconnects.
 */
export class AdminPool {
  private readonly createClient: KafkaClientFactory;
  private readonly pooled = new Map<string, Promise<PooledAdmin>>();

  constructor(createClient: KafkaClientFactory) {
    this.createClient = createClient;
  }

  get(profileName: string | null): Promise<PooledAdmin> {
    const key = keyFor(profileName);
    const existing = this.pooled.get(key);
    if (existing !== undefined) return existing;

    const connecting = this.connect(profileName);
    this.pooled.set(key, connecting);
    void connecting.catch(() => this.pooled.delete(key));
    return connecting;
  }

  private async connect(profileName: string | null): Promise<PooledAdmin> {
    const admin = this.createClient(profileName).admin();
    await admin.connect();
    return admin;
  }

  /** Disconnects and forgets the pooled admin for one profile, if any — a no-op when nothing was pooled for it. */
  async invalidate(profileName: string | null): Promise<void> {
    const key = keyFor(profileName);
    const pending = this.pooled.get(key);
    this.pooled.delete(key);
    if (pending === undefined) return;

    const admin = await pending.catch(() => null);
    await admin?.disconnect();
  }

  /** Disconnects every pooled admin — called once, on server shutdown. */
  async disposeAll(): Promise<void> {
    const pending = [...this.pooled.values()];
    this.pooled.clear();
    await Promise.all(
      pending.map(async (entry) => {
        const admin = await entry.catch(() => null);
        await admin?.disconnect();
      }),
    );
  }
}
