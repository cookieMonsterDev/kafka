import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createFakeEventSource, type StreamOpener } from './fake-event-source';

const BASE = 'http://studio.test/demo/index.html';

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('createFakeEventSource', () => {
  it('opens asynchronously and delivers named frames as JSON message events', () => {
    let emit: Parameters<StreamOpener>[1] = () => {};
    const FakeEventSource = createFakeEventSource((url, send) => {
      expect(url.toString()).toBe('http://studio.test/api/events');
      emit = send;
      return () => {};
    }, BASE);
    const source = new FakeEventSource('/api/events');
    const opened = vi.fn();
    const received: unknown[] = [];
    source.addEventListener('open', opened);
    source.addEventListener('activity', (event) => received.push(JSON.parse((event as MessageEvent<string>).data)));

    expect(source.readyState).toBe(FakeEventSource.CONNECTING);
    vi.runAllTimers();
    emit('activity', { id: 1 });

    expect(opened).toHaveBeenCalledOnce();
    expect(source.readyState).toBe(FakeEventSource.OPEN);
    expect(received).toEqual([{ id: 1 }]);
  });

  it('delivers frames sent while the stream starts after open, then ends it if asked', () => {
    const FakeEventSource = createFakeEventSource((_url, send, end) => {
      send('tail-error', { message: 'gone' });
      end();
      return () => {};
    }, BASE);
    const source = new FakeEventSource('/api/topics/missing/tail');
    const order: string[] = [];
    source.addEventListener('open', () => order.push('open'));
    source.addEventListener('tail-error', () => order.push('tail-error'));
    source.addEventListener('error', () => order.push('error'));

    vi.runAllTimers();

    expect(order).toEqual(['open', 'tail-error', 'error']);
    expect(source.readyState).toBe(FakeEventSource.CLOSED);
  });

  it('closes for good and fires error when the stream ends or does not exist', () => {
    const FakeEventSource = createFakeEventSource(() => null, BASE);
    const source = new FakeEventSource('/api/unknown');
    const onerror = vi.fn();
    source.onerror = onerror;

    vi.runAllTimers();

    expect(onerror).toHaveBeenCalledOnce();
    expect(source.readyState).toBe(FakeEventSource.CLOSED);
  });

  it('runs the stream cleanup when the client closes it', () => {
    const cleanup = vi.fn();
    const FakeEventSource = createFakeEventSource(() => cleanup, BASE);
    const source = new FakeEventSource('/api/events');
    vi.runAllTimers();

    source.close();
    source.close();

    expect(cleanup).toHaveBeenCalledOnce();
  });
});
