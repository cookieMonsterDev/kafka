interface ConnectedClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
}

/** Shares one pending connection per profile and owns its invalidation and shutdown. */
export class ProfileClientPool<Client extends ConnectedClient> {
  private readonly createClient: (profile: string | null) => Client;
  private readonly pooled = new Map<string | null, Promise<Client>>();

  constructor(createClient: (profile: string | null) => Client) {
    this.createClient = createClient;
  }

  get(profile: string | null): Promise<Client> {
    const existing = this.pooled.get(profile);
    if (existing !== undefined) return existing;
    const connecting = this.connect(profile);
    this.pooled.set(profile, connecting);
    void connecting.catch(() => {
      // An invalidated connection may fail after its replacement has already been installed.
      if (this.pooled.get(profile) === connecting) this.pooled.delete(profile);
    });
    return connecting;
  }

  private async connect(profile: string | null): Promise<Client> {
    const client = this.createClient(profile);
    try {
      await client.connect();
      return client;
    } catch (error) {
      await client.disconnect().catch(() => {});
      throw error;
    }
  }

  /** Forget immediately; a pending connection is disconnected when it settles. */
  async invalidate(profile: string | null): Promise<void> {
    const pending = this.pooled.get(profile);
    this.pooled.delete(profile);
    const client = await pending?.catch(() => null);
    await client?.disconnect();
  }

  /** Disconnect all current entries, including connections that are still pending. */
  async disposeAll(): Promise<void> {
    const pending = [...this.pooled.values()];
    this.pooled.clear();
    await Promise.all(
      pending.map(async (entry) => {
        const client = await entry.catch(() => null);
        await client?.disconnect();
      }),
    );
  }
}
