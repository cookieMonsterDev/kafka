import { describe, expect, it } from 'vitest';
import { RingBuffer } from './ring-buffer';

describe('RingBuffer', () => {
  it('accumulates pushed items in order', () => {
    const buffer = new RingBuffer<number>(5);
    buffer.push(1);
    buffer.push(2);
    expect(buffer.getSnapshot()).toEqual([1, 2]);
  });

  it('drops the oldest item once past capacity', () => {
    const buffer = new RingBuffer<number>(3);
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);
    buffer.push(4);
    expect(buffer.getSnapshot()).toEqual([2, 3, 4]);
  });

  it('clear() empties the buffer', () => {
    const buffer = new RingBuffer<number>(3);
    buffer.push(1);
    buffer.clear();
    expect(buffer.getSnapshot()).toEqual([]);
  });

  it('notifies subscribers on push and on clear', () => {
    const buffer = new RingBuffer<number>(3);
    let notifications = 0;
    const unsubscribe = buffer.subscribe(() => {
      notifications += 1;
    });

    buffer.push(1);
    buffer.clear();
    expect(notifications).toBe(2);

    unsubscribe();
    buffer.push(2);
    expect(notifications).toBe(2);
  });

  it('getSnapshot() returns the same reference until the next mutation', () => {
    const buffer = new RingBuffer<number>(3);
    buffer.push(1);
    const first = buffer.getSnapshot();
    const second = buffer.getSnapshot();
    expect(first).toBe(second);
  });
});
