import { describe, expect, it } from 'vitest';
import { ResolvedOffsets } from './resolved-offsets';

describe('consumer/offset-manager/resolved-offsets', () => {
  it('exposes the offset after the last resolved one, latest call winning', () => {
    const offsets = new ResolvedOffsets();
    offsets.resolve('events', 0, 4n);
    offsets.resolve('events', 0, 9n);
    offsets.resolve('events', 1, 0n);

    expect(offsets.snapshot()['events']).toEqual({ 0: 10n, 1: 1n });
  });

  it('applies later resolves on top of edits made through a snapshot', () => {
    const offsets = new ResolvedOffsets();
    offsets.resolve('events', 0, 4n);
    offsets.snapshot()['events']![0] = 2n;
    expect(offsets.snapshot()['events']![0]).toBe(2n);

    offsets.resolve('events', 0, 7n);
    expect(offsets.snapshot()['events']![0]).toBe(8n);
  });

  it('drops pending offsets on replace', () => {
    const offsets = new ResolvedOffsets();
    offsets.resolve('events', 0, 4n);
    offsets.replace({ events: {} });

    expect(offsets.snapshot()).toEqual({ events: {} });
  });

  it('keeps a topic named __proto__ as an ordinary key', () => {
    const offsets = new ResolvedOffsets();
    offsets.resolve('__proto__', 0, 1n);

    const snapshot = offsets.snapshot();
    expect(Object.keys(snapshot)).toEqual(['__proto__']);
    expect(snapshot['__proto__']?.[0]).toBe(2n);
    expect(({} as Record<string, unknown>)['0']).toBeUndefined();
  });
});
