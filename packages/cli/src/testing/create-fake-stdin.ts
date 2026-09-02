import { vi } from 'vitest';
import type { Runtime } from '../runtime';

export type FakeStdin = Runtime['stdin'] & {
  emitData(chunk: string): void;
  emitEnd(): void;
};

/** A `Runtime['stdin']` a test can push bytes through, for commands that read a piped secret. */
export function createFakeStdin(): FakeStdin {
  const dataListeners: ((chunk: string) => void)[] = [];
  const endListeners: (() => void)[] = [];
  return {
    setEncoding: vi.fn(),
    on: vi.fn((event: 'data' | 'end', listener: ((chunk: string) => void) | (() => void)) => {
      if (event === 'data') dataListeners.push(listener);
      else endListeners.push(listener as () => void);
    }),
    emitData(chunk: string) {
      for (const listener of dataListeners) listener(chunk);
    },
    emitEnd() {
      for (const listener of endListeners) listener();
    },
  };
}
