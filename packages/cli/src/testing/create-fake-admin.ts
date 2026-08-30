import type { Admin } from '@cookiemonsterdev/kafka-core';

export type FakeAdminOverrides = Partial<Admin>;

/**
 * An `Admin` where every method throws unless explicitly stubbed — a command test only stubs the
 * calls its scenario actually needs, so an unexpected call (the wrong method, called with the
 * wrong options) fails immediately and by name, without ever touching a real broker.
 */
export function createFakeAdmin(overrides: FakeAdminOverrides = {}): Admin {
  const handler: ProxyHandler<Admin> = {
    get(_target, prop) {
      if (prop === Symbol.asyncDispose) {
        return async () => {};
      }
      // Without this, `await fakeAdmin` treats the proxy as a thenable (any property access
      // looks like a method) and calls a throwing "then" as if it were a promise executor.
      if (prop === 'then') {
        return undefined;
      }
      if (Object.hasOwn(overrides, prop)) {
        return (overrides as Record<PropertyKey, unknown>)[prop as string];
      }
      return (...args: unknown[]) => {
        throw new Error(
          `createFakeAdmin: unstubbed call to Admin.${String(prop)}(${args.map((arg) => JSON.stringify(arg)).join(', ')})`,
        );
      };
    },
  };
  return new Proxy({} as Admin, handler);
}
