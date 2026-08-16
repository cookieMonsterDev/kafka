import { describe, expect, it, vi } from 'vitest';
import { once } from './once';

describe('utils/once', () => {
  it('calls the wrapped function only once', () => {
    const original = vi.fn().mockReturnValue('foo');
    const wrapped = once(original);

    expect(wrapped('hello')).toEqual('foo');
    expect(wrapped('hello')).toBeUndefined();

    expect(original).toHaveBeenCalledTimes(1);
    expect(original).toHaveBeenCalledWith('hello');
  });
});
