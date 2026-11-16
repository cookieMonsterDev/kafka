import { describe, expect, it } from 'vitest';
import * as lazyFamilies from './lazy-families';
import * as lazyRequests from './lazy-requests';

describe('protocol/requests/lazy-families', () => {
  it('has one stub for each family in the lazy-requests chunk', () => {
    expect(Object.keys(lazyFamilies).sort()).toEqual(Object.keys(lazyRequests).sort());
  });

  it.each(Object.entries(lazyFamilies))('%s loads the family for the API it names', async (name, stub) => {
    const family = await stub.load();

    expect(family).toBe(lazyRequests[name as keyof typeof lazyRequests]);
    expect({ apiKey: family.apiKey, apiName: family.apiName }).toEqual({ apiKey: stub.apiKey, apiName: stub.apiName });
    expect(family.firstFlexibleVersion).toBe(stub.firstFlexibleVersion);
  });
});
