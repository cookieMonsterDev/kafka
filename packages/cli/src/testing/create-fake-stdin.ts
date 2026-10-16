import { vi, type Mock } from 'vitest';

type ListenerMock = Mock<(event: 'data' | 'end', listener: (chunk: string) => void) => void>;

/** Mocks as properties, not methods, so a test can assert on them without unbinding a method. */
export interface FakeStdin {
  readonly isTTY?: boolean;
  readonly readableEnded?: boolean;
  readonly setEncoding: Mock<(encoding: BufferEncoding) => void>;
  readonly on: ListenerMock;
  readonly removeListener: ListenerMock;
  readonly pause: Mock<() => void>;
  readonly emitData: (chunk: string) => void;
  readonly emitEnd: () => void;
  readonly listenerCount: () => number;
}

/** A `Runtime['stdin']` a test can push bytes through, for commands that read a piped secret. */
export function createFakeStdin(options: { isTTY?: boolean; readableEnded?: boolean } = {}): FakeStdin {
  const listeners = new Map<(chunk: string) => void, 'data' | 'end'>();
  const emit = (event: 'data' | 'end', chunk = '') => {
    for (const [listener, listenerEvent] of [...listeners]) if (listenerEvent === event) listener(chunk);
  };
  return {
    isTTY: options.isTTY,
    readableEnded: options.readableEnded,
    setEncoding: vi.fn(),
    on: vi.fn((event: 'data' | 'end', listener: (chunk: string) => void) => {
      listeners.set(listener, event);
    }),
    removeListener: vi.fn((_event: 'data' | 'end', listener: (chunk: string) => void) => {
      listeners.delete(listener);
    }),
    pause: vi.fn(),
    emitData: (chunk: string) => emit('data', chunk),
    emitEnd: () => emit('end'),
    listenerCount: () => listeners.size,
  };
}
