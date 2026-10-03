import { describe, expect, it } from 'vitest';
import { unsupportedRuntimeMessage } from './runtime-version';

describe('unsupportedRuntimeMessage', () => {
  it('accepts Node.js 24 and newer', () => {
    expect(unsupportedRuntimeMessage({ node: '24.0.0' })).toBeUndefined();
    expect(unsupportedRuntimeMessage({ node: '26.9.0' })).toBeUndefined();
  });

  it('rejects Node.js older than 24', () => {
    expect(unsupportedRuntimeMessage({ node: '22.12.0' })).toBe('kafka: requires Node.js >=24.0.0, found 22.12.0');
  });

  it('accepts Bun 1.4.0 and newer', () => {
    expect(unsupportedRuntimeMessage({ node: '26.3.0', bun: '1.4.0' })).toBeUndefined();
    expect(unsupportedRuntimeMessage({ node: '26.3.0', bun: '1.4.2' })).toBeUndefined();
    expect(unsupportedRuntimeMessage({ node: '26.3.0', bun: '1.10.0' })).toBeUndefined();
    expect(unsupportedRuntimeMessage({ node: '26.3.0', bun: '2.0.0' })).toBeUndefined();
  });

  it('rejects Bun older than 1.4.0', () => {
    expect(unsupportedRuntimeMessage({ node: '24.3.0', bun: '1.3.9' })).toBe(
      'kafka: requires Bun >=1.4.0, found 1.3.9',
    );
  });

  it('checks the Bun version even when the reported Node.js compatibility version is new enough', () => {
    expect(unsupportedRuntimeMessage({ node: '26.3.0', bun: '1.3.9' })).toBe(
      'kafka: requires Bun >=1.4.0, found 1.3.9',
    );
  });

  it('accepts a supported Bun even when the reported Node.js compatibility version is older', () => {
    expect(unsupportedRuntimeMessage({ node: '22.6.0', bun: '1.4.2' })).toBeUndefined();
  });
});
