import { describe, expect, it } from 'vitest';
import { greet, packageInfo } from './index.js';

describe('index', () => {
  it('exposes package info', () => {
    expect(packageInfo.name).toBe('@kafka/core');
  });

  it('greets', () => {
    expect(greet('world')).toBe('@kafka/core says hello to world');
  });
});
