import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import type { StudioConnectionConfig } from '../kafka/connection';
import { createFakeAdmin } from '../kafka/create-fake-admin';
import { Router } from '../router';
import { registerProfileRoutes, type ProfilesRouteContext } from './profiles';

function fakePool(): AdminPool {
  return new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {} }),
  }));
}

function buildContext(overrides: Partial<ProfilesRouteContext> = {}): ProfilesRouteContext {
  const connection: StudioConnectionConfig = {
    path: null,
    fileConfig: null,
    env: {},
    profiles: {
      staging: { brokers: ['staging:9092'] },
      prod: { brokers: ['prod:9092'], sasl: { password: 'sekret' } },
    },
  };
  let active: string | null = null;
  return {
    connection,
    pool: fakePool(),
    getActiveProfile: () => active,
    setActiveProfile: (profile) => {
      active = profile;
    },
    ...overrides,
  };
}

async function withServer<T>(context: ProfilesRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerProfileRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerProfileRoutes', () => {
  it('GET /api/profiles lists profiles with active null and secrets redacted', async () => {
    const context = buildContext();
    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/profiles`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        active: string | null;
        profiles: Record<string, { sasl?: { password: string } }>;
      };
      expect(body.active).toBeNull();
      expect(Object.keys(body.profiles)).toEqual(['staging', 'prod']);
      expect(body.profiles.prod?.sasl?.password).toBe('[REDACTED]');
    });
  });

  it('POST /api/profiles/active switches the active profile', async () => {
    const context = buildContext();
    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'staging' }),
      });
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toMatchObject({ active: 'staging' });
      expect(context.getActiveProfile()).toBe('staging');
    });
  });

  it('POST /api/profiles/active accepts null to switch back to the direct connection', async () => {
    const context = buildContext();
    context.setActiveProfile('staging');
    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: null }),
      });
      expect(res.status).toBe(200);
      expect(context.getActiveProfile()).toBeNull();
    });
  });

  it('POST /api/profiles/active rejects an unknown profile with 404', async () => {
    const context = buildContext();
    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'nope' }),
      });
      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: { code: string; details: { available: string[] } } };
      expect(body.error.code).toBe('unknown_profile');
      expect(body.error.details.available).toEqual(['staging', 'prod']);
      expect(context.getActiveProfile()).toBeNull();
    });
  });

  it('POST /api/profiles/active rejects a non-string, non-null profile with 400', async () => {
    const context = buildContext();
    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 42 }),
      });
      expect(res.status).toBe(400);
    });
  });

  it('invalidates the pooled admin for the profile being switched away from', async () => {
    const context = buildContext();
    context.setActiveProfile('staging');
    const invalidate = vi.spyOn(context.pool, 'invalidate');

    await withServer(context, async (baseUrl) => {
      await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'prod' }),
      });
    });

    expect(invalidate).toHaveBeenCalledWith('staging');
  });

  it('does not invalidate anything when re-selecting the already-active profile', async () => {
    const context = buildContext();
    context.setActiveProfile('staging');
    const invalidate = vi.spyOn(context.pool, 'invalidate');

    await withServer(context, async (baseUrl) => {
      await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'staging' }),
      });
    });

    expect(invalidate).not.toHaveBeenCalled();
  });

  it('warms the new profile in the background after switching to it', async () => {
    const context = buildContext();
    const get = vi.spyOn(context.pool, 'get');

    await withServer(context, async (baseUrl) => {
      await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'staging' }),
      });
    });

    expect(get).toHaveBeenCalledWith('staging');
  });

  it('does not warm anything when re-selecting the already-active profile', async () => {
    const context = buildContext();
    context.setActiveProfile('staging');
    const get = vi.spyOn(context.pool, 'get');

    await withServer(context, async (baseUrl) => {
      await fetch(`${baseUrl}/api/profiles/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile: 'staging' }),
      });
    });

    expect(get).not.toHaveBeenCalled();
  });
});
