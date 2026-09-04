import { describe, expect, it } from 'vitest';
import { assertResolvedFileConfig } from './resolve-module';

describe('assertResolvedFileConfig', () => {
  it("includes assertValid's thrown message when the value is a plain object but fails a section-aware check", () => {
    const path = '/kafka.config.ts';
    const assertValid = (value: unknown): void => {
      const port = (value as { client?: { port?: unknown } }).client?.port;
      if (typeof port !== 'number') {
        throw new TypeError('port must be a number');
      }
    };

    let thrown: unknown;
    try {
      assertResolvedFileConfig({ client: { port: '9092' } }, path, assertValid);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toEqual(expect.objectContaining({ tag: 'ConfigFileInvalid', path }));
    const message = (thrown as Error).message;
    expect(message).toContain('port must be a number');
    expect(message).not.toContain('must export an object');
  });
});
