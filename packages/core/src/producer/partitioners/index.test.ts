import { describe, expect, it } from 'vitest';
import { DefaultPartitioner, LegacyPartitioner, StickyPartitioner } from './index';

describe('producer/partitioners', () => {
  it('exposes the default, legacy, and sticky partitioners', () => {
    expect(DefaultPartitioner).toBeTypeOf('function');
    expect(LegacyPartitioner).toBeTypeOf('function');
    expect(StickyPartitioner).toBeTypeOf('function');
    expect(LegacyPartitioner).not.toBe(DefaultPartitioner);
    expect(StickyPartitioner).not.toBe(DefaultPartitioner);
  });
});
