import { useSyncExternalStore } from 'react';

/**
 * A capped FIFO with a `useSyncExternalStore`-compatible subscribe/snapshot pair. High-frequency
 * data (the board's activity firehose) reads this two ways: React components subscribe through
 * {@link useRingBuffer} for the odd re-render (a counter, a recent-activity list), while the canvas
 * particle layer reads {@link getSnapshot} directly inside its own `requestAnimationFrame` loop —
 * one push must never force a React re-render on every single event, or the loop would fight React
 * for the frame.
 */
export class RingBuffer<T> {
  private readonly capacity: number;
  private items: readonly T[] = [];
  private readonly listeners = new Set<() => void>();

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  push(item: T): void {
    const next = [...this.items, item];
    this.items = next.length > this.capacity ? next.slice(next.length - this.capacity) : next;
    for (const listener of this.listeners) listener();
  }

  clear(): void {
    this.items = [];
    for (const listener of this.listeners) listener();
  }

  getSnapshot = (): readonly T[] => this.items;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
}

export function useRingBuffer<T>(buffer: RingBuffer<T>): readonly T[] {
  return useSyncExternalStore(buffer.subscribe, buffer.getSnapshot, buffer.getSnapshot);
}
