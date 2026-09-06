import type { PooledProducer } from './produce';

export type FakeProducerOverrides = Partial<PooledProducer>;

/**
 * A {@link PooledProducer} where every method throws unless explicitly stubbed — mirrors
 * {@link createFakeAdmin} (`./create-fake-admin`) for the same reason: a test only stubs the calls
 * its scenario actually needs, so an unexpected call fails immediately and by name.
 */
export function createFakeProducer(overrides: FakeProducerOverrides = {}): PooledProducer {
  const handler: ProxyHandler<PooledProducer> = {
    get(_target, prop) {
      if (prop === 'then') return undefined;
      if (Object.hasOwn(overrides, prop)) {
        return (overrides as Record<PropertyKey, unknown>)[prop as string];
      }
      return (...args: unknown[]) => {
        throw new Error(
          `createFakeProducer: unstubbed call to Producer.${String(prop)}(${args.map((arg) => JSON.stringify(arg)).join(', ')})`,
        );
      };
    },
  };
  return new Proxy({} as PooledProducer, handler);
}
