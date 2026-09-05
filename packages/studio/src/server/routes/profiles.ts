import { redactKafkaConfig } from '@cookiemonsterdev/kafka-core';
import { sendError, sendJson } from '../create-server';
import { isKnownProfile, listProfileNames, type StudioConnectionConfig } from '../kafka/connection';
import { readJsonBody } from '../json';
import type { AdminPool } from '../kafka/admin-pool';
import type { Router } from '../router';

export interface ProfilesRouteContext {
  readonly connection: StudioConnectionConfig;
  readonly pool: AdminPool;
  getActiveProfile(): string | null;
  setActiveProfile(profile: string | null): void;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Never serializes a secret: every profile is a plain `KafkaConfig`-shaped object with no restriction on what it holds. */
function profilesPayload(context: ProfilesRouteContext): { active: string | null; profiles: unknown } {
  return {
    active: context.getActiveProfile(),
    profiles: redactKafkaConfig(context.connection.profiles),
  };
}

/**
 * `GET /api/profiles` lists the named connection profiles (redacted), and `POST
 * /api/profiles/active` switches which one is active. Switching disconnects and forgets any
 * pooled admin for the profile being switched away from — the studio only ever needs one live
 * connection at a time — and starts warming the new profile's connection in the background so the
 * first real request against it doesn't pay the connect cost.
 */
export function registerProfileRoutes(router: Router, context: ProfilesRouteContext): void {
  router.get('/api/profiles', (_req, res) => {
    sendJson(res, 200, profilesPayload(context));
  });

  router.post('/api/profiles/active', async (req, res) => {
    const body = await readJsonBody(req);
    const requested = isPlainObject(body) ? body.profile : undefined;

    if (requested !== null && typeof requested !== 'string') {
      sendError(res, 400, 'bad_request', '"profile" must be a string or null');
      return;
    }

    if (requested !== null && !isKnownProfile(context.connection, requested)) {
      sendError(res, 404, 'unknown_profile', `unknown profile "${requested}"`, {
        available: listProfileNames(context.connection),
      });
      return;
    }

    const previous = context.getActiveProfile();
    context.setActiveProfile(requested);
    if (previous !== requested) {
      await context.pool.invalidate(previous);
      // Fire-and-forget: a failed warm-up doesn't fail the switch — the pool discards a failed
      // entry itself, so whatever calls get() next for real just retries and surfaces the error.
      void context.pool.get(requested).catch(() => {});
    }

    sendJson(res, 200, profilesPayload(context));
  });
}
