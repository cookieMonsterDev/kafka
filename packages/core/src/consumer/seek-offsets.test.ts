import { describe, expect, it } from 'vitest';
import { SeekOffsets } from './seek-offsets';

describe('consumer/seek-offsets', () => {
  it('stores the latest offset per topic-partition and reports has()', () => {
    const seeks = new SeekOffsets();
    seeks.set('topic-a', 0, 10n);
    seeks.set('topic-a', 1, 20n);
    expect(seeks.has('topic-a', 0)).toBe(true);
    expect(seeks.has('topic-a', 1)).toBe(true);
    expect(seeks.has('topic-b', 0)).toBe(false);
    expect(seeks.size).toBe(2);
  });

  it('overwrites a previous seek for the same topic-partition', () => {
    const seeks = new SeekOffsets();
    seeks.set('topic-a', 0, 10n);
    seeks.set('topic-a', 0, 99n);
    expect(seeks.pop('topic-a', 0)).toEqual({ topic: 'topic-a', partition: 0, offset: 99n });
    expect(seeks.has('topic-a', 0)).toBe(false);
    expect(seeks.size).toBe(0);
  });

  it('pop returns undefined when the pair is missing', () => {
    const seeks = new SeekOffsets();
    expect(seeks.pop('topic-a', 0)).toBeUndefined();
  });
});
