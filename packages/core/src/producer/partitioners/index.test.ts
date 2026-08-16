import { describe, expect, it } from 'vitest';
import { DefaultPartitioner, JavaCompatiblePartitioner, LegacyPartitioner } from './index.js';

describe('producer/partitioners', () => {
  it('exposes the default and legacy partitioners', () => {
    expect(DefaultPartitioner).toBeTypeOf('function');
    expect(LegacyPartitioner).toBeTypeOf('function');
    expect(LegacyPartitioner).not.toBe(DefaultPartitioner);
  });

  it('keeps JavaCompatiblePartitioner as an alias of DefaultPartitioner', () => {
    expect(JavaCompatiblePartitioner).toBe(DefaultPartitioner);
  });
});
