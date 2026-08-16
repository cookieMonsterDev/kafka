export interface SeekEntry {
  topic: string;
  partition: number;
  offset: bigint;
}

/**
 * Pending `seek()` calls, keyed by topic+partition. The latest seek for a pair wins; `pop`
 * removes and returns it so the next fetch applies it exactly once.
 */
export class SeekOffsets {
  readonly #offsets = new Map<string, bigint>();

  get size(): number {
    return this.#offsets.size;
  }

  getKey(topic: string, partition: number): string {
    return JSON.stringify([topic, partition]);
  }

  set(topic: string, partition: number, offset: bigint): this {
    this.#offsets.set(this.getKey(topic, partition), offset);
    return this;
  }

  has(topic: string, partition: number): boolean {
    return this.#offsets.has(this.getKey(topic, partition));
  }

  pop(topic: string, partition: number): SeekEntry | undefined {
    if (this.#offsets.size === 0 || !this.has(topic, partition)) {
      return undefined;
    }

    const key = this.getKey(topic, partition);
    const offset = this.#offsets.get(key);
    this.#offsets.delete(key);
    if (offset === undefined) return undefined;
    return { topic, partition, offset };
  }
}
