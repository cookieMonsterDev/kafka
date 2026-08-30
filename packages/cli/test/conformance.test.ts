import { describe, expect, it } from 'vitest';
import {
  ADMIN_METHOD_CLASSIFICATION,
  type AdminMethodName,
  type MethodClassification,
} from '../src/admin/method-classification';

/**
 * `ADMIN_METHOD_CLASSIFICATION`'s own type — `Record<AdminMethodName, MethodClassification>` —
 * is already the completeness guarantee: `AdminMethodName` is derived directly from core's real
 * `Admin` interface (minus lifecycle/introspection members), so a method added to `Admin` without
 * an entry here fails `tsc`, not this test. What's left to check at runtime is the bucket
 * contract itself: every classified method lands in `mounted` or `passthrough-only`, and
 * `out-of-scope` — the third bucket the admin surface is required to keep empty — has nothing in
 * it, by construction of `MethodClassification` not even naming that value.
 */
describe('the admin method conformance table', () => {
  const buckets: Record<MethodClassification, AdminMethodName[]> = { mounted: [], 'passthrough-only': [] };
  for (const [name, classification] of Object.entries(ADMIN_METHOD_CLASSIFICATION) as [
    AdminMethodName,
    MethodClassification,
  ][]) {
    buckets[classification].push(name);
  }

  it('has at least one mounted method', () => {
    expect(buckets.mounted.length).toBeGreaterThan(0);
  });

  it('has at least one passthrough-only method', () => {
    expect(buckets['passthrough-only'].length).toBeGreaterThan(0);
  });

  it('classifies every method into exactly one of the two known buckets', () => {
    const classified = new Set(Object.keys(ADMIN_METHOD_CLASSIFICATION));
    const bucketed = new Set([...buckets.mounted, ...buckets['passthrough-only']]);
    expect(bucketed).toEqual(classified);
  });

  it.each(Object.entries(ADMIN_METHOD_CLASSIFICATION))(
    '"%s" is mounted or passthrough-only, never out-of-scope',
    (_name, classification) => {
      expect(['mounted', 'passthrough-only']).toContain(classification);
    },
  );
});
