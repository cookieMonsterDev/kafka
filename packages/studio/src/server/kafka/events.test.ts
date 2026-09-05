import { describe, expect, it } from 'vitest';
import { StudioEventBus } from './events';

describe('StudioEventBus', () => {
  it('assigns an incrementing id and a timestamp to each published event', () => {
    const bus = new StudioEventBus();
    const received: unknown[] = [];
    bus.subscribe((event) => received.push(event));

    bus.publish({ kind: 'produce', topic: 'orders', partition: 0, count: 1, bytes: 3 });
    bus.publish({ kind: 'consume', topic: 'orders', partition: 0, count: 1, bytes: 3 });

    expect(received).toEqual([
      expect.objectContaining({ id: 1, kind: 'produce' }),
      expect.objectContaining({ id: 2, kind: 'consume' }),
    ]);
  });

  it('delivers a published event to every subscriber', () => {
    const bus = new StudioEventBus();
    const first: unknown[] = [];
    const second: unknown[] = [];
    bus.subscribe((event) => first.push(event));
    bus.subscribe((event) => second.push(event));

    bus.publish({ kind: 'produce', topic: 'orders', partition: null, count: 1, bytes: 3 });

    expect(first).toHaveLength(1);
    expect(second).toHaveLength(1);
  });

  it('stops delivering to a subscriber once unsubscribed', () => {
    const bus = new StudioEventBus();
    const received: unknown[] = [];
    const unsubscribe = bus.subscribe((event) => received.push(event));
    unsubscribe();

    bus.publish({ kind: 'produce', topic: 'orders', partition: null, count: 1, bytes: 3 });

    expect(received).toEqual([]);
  });

  it('hasListeners() reflects whether any subscriber is attached', () => {
    const bus = new StudioEventBus();
    expect(bus.hasListeners()).toBe(false);

    const unsubscribe = bus.subscribe(() => {});
    expect(bus.hasListeners()).toBe(true);

    unsubscribe();
    expect(bus.hasListeners()).toBe(false);
  });

  it('is a no-op to publish() with no subscribers', () => {
    const bus = new StudioEventBus();
    expect(() => bus.publish({ kind: 'produce', topic: 'orders', partition: null, count: 1, bytes: 3 })).not.toThrow();
  });
});
