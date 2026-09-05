import type { StudioEvent } from '../../shared/contracts/event';

export type StudioEventInput = Omit<StudioEvent, 'id' | 'timestamp'>;

/**
 * Fans out the studio's own produce/tail activity to every open `GET /api/events` connection —
 * the board's particle layer and recent-activity feed read from this, never from a decorative
 * timer. There is no history: a listener only ever sees events published while it is subscribed,
 * matching the SSE tail's own "from now on" semantics.
 */
export class StudioEventBus {
  private readonly listeners = new Set<(event: StudioEvent) => void>();
  private nextId = 1;

  hasListeners(): boolean {
    return this.listeners.size > 0;
  }

  publish(input: StudioEventInput): void {
    // Building the event is cheap, but skipping it entirely when nobody is listening mirrors the
    // core instrumentation emitter's own "listenerCount > 0" guard.
    if (this.listeners.size === 0) return;
    const event: StudioEvent = { ...input, id: this.nextId, timestamp: Date.now() };
    this.nextId += 1;
    for (const listener of this.listeners) listener(event);
  }

  subscribe(listener: (event: StudioEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}
