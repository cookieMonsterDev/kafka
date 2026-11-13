type OffsetsByTopic = Record<string, Record<string, bigint>>;

/** Prototype-free record, so a topic named `__proto__` (legal in Kafka) stays an ordinary key. */
function createRecord<Value>(): Record<string, Value> {
  return Object.create(null) as Record<string, Value>;
}

/**
 * Next offset to commit per topic and partition. Kafka commits the next offset to read, so
 * `resolve()` stores the last processed offset and defers the `+ 1n` until `snapshot()`: the
 * per-record path then allocates nothing once a partition has been seen.
 */
export class ResolvedOffsets {
  #resolved: OffsetsByTopic = createRecord();
  readonly #pending = new Map<string, Map<number, bigint>>();
  #hasPending = false;

  resolve(topic: string, partition: number, offset: bigint): void {
    let pendingTopic = this.#pending.get(topic);
    if (pendingTopic === undefined) {
      pendingTopic = new Map();
      this.#pending.set(topic, pendingTopic);
    }
    pendingTopic.set(partition, offset);
    this.#hasPending = true;
  }

  /** Applies pending offsets and returns the map; callers may edit it until the next `resolve()`. */
  snapshot(): OffsetsByTopic {
    if (!this.#hasPending) return this.#resolved;

    for (const [topic, pendingTopic] of this.#pending) {
      if (pendingTopic.size === 0) continue;
      const resolvedTopic = (this.#resolved[topic] ??= createRecord());
      for (const [partition, offset] of pendingTopic) {
        resolvedTopic[partition] = offset + 1n;
      }
      // Cleared rather than deleted, so the next resolve() for this topic reuses the map.
      pendingTopic.clear();
    }
    this.#hasPending = false;
    return this.#resolved;
  }

  /** Drops pending offsets and replaces the map with a prototype-free copy of `resolved`. */
  replace(resolved: OffsetsByTopic): void {
    for (const pendingTopic of this.#pending.values()) {
      pendingTopic.clear();
    }
    this.#hasPending = false;
    this.#resolved = Object.assign(createRecord<Record<string, bigint>>(), resolved);
  }
}
