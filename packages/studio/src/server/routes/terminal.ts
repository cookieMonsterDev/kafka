import { embeddedCliConfig } from '@cookiemonsterdev/kafka-cli/embedded';
import { studioTerminalCommands } from '../terminal/commands';
import type { ProducerPool } from '../kafka/produce';
import type { MessageConsumerFactory } from '../kafka/messages';
import type { StudioEventBus } from '../kafka/events';
import { z } from 'zod';
import { readJsonBody } from '../json';
import { sendError, sendJson } from '../create-server';
import { openSseStream } from '../sse';
import type { Router } from '../router';
import type { AdminPool } from '../kafka/admin-pool';
import { createKafkaClient, type StudioConnectionConfig } from '../kafka/connection';
import { TerminalSession } from '../terminal/session';

const inputSchema = z.object({ data: z.string().max(8192) });
const resizeSchema = z.object({ columns: z.number().int().min(20).max(500) });

export function registerTerminalRoutes(
  router: Router,
  context: {
    readonly connection: StudioConnectionConfig;
    readonly pool: AdminPool;
    readonly readOnly: boolean;
    readonly producers: ProducerPool;
    readonly consumerFactory: MessageConsumerFactory;
    readonly events: StudioEventBus;
    getActiveProfile(): string | null;
  },
): { dispose(): void } {
  const sessions = new Map<string, TerminalSession>();
  router.post(
    '/api/terminal/sessions',
    (_req, res) => {
      if (sessions.size >= 8) {
        sendError(res, 429, 'session_limit', 'Close an existing terminal session first.');
        return;
      }
      const profile = context.getActiveProfile();
      const session = new TerminalSession({
        profile,
        readOnly: context.readOnly,
        config: embeddedCliConfig(context.connection.fileConfig, profile, context.connection.path),
        commands: studioTerminalCommands({ ...context, profile }),
        createAdmin: () => createKafkaClient(context.connection, profile).admin(),
        listResources: async () => {
          const admin = await context.pool.get(profile);
          const [topics, groups] = await Promise.all([admin.listTopics(), admin.listGroups()]);
          return { topics, groups: groups.groups.map((group) => group.groupId) };
        },
        onClose: () => sessions.delete(session.id),
      });
      sessions.set(session.id, session);
      sendJson(res, 201, { id: session.id, profile, readOnly: context.readOnly });
    },
    { allowInReadOnly: true },
  );

  router.get('/api/terminal/sessions/:id/events', (req, res, params) => {
    const session = sessions.get(params.id ?? '');
    if (session === undefined) {
      sendError(res, 404, 'session_closed', 'Terminal session has ended.');
      return;
    }
    let unsubscribe = () => {};
    const stream = openSseStream(req, res, () => unsubscribe());
    unsubscribe = session.subscribe((data) => {
      stream.send('output', { data });
      if (session.isClosed) {
        stream.send('exit', {});
        stream.close();
      }
    });
  });

  router.post(
    '/api/terminal/sessions/:id/input',
    async (req, res, params) => {
      const parsed = inputSchema.safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'data must be a string of at most 8192 characters.');
        return;
      }
      const session = sessions.get(params.id ?? '');
      if (session === undefined) {
        sendError(res, 404, 'session_closed', 'Terminal session has ended.');
        return;
      }
      if (session.profile !== context.getActiveProfile()) {
        sendError(res, 409, 'profile_changed', 'Connection profile changed. Reopen the terminal.');
        return;
      }
      session.input(parsed.data.data);
      sendJson(res, 200, { ok: true });
    },
    { allowInReadOnly: true },
  );

  router.post(
    '/api/terminal/sessions/:id/resize',
    async (req, res, params) => {
      const parsed = resizeSchema.safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'columns must be between 20 and 500.');
        return;
      }
      const session = sessions.get(params.id ?? '');
      if (session === undefined) {
        sendError(res, 404, 'session_closed', 'Terminal session has ended.');
        return;
      }
      session.resize(parsed.data.columns);
      sendJson(res, 200, { ok: true });
    },
    { allowInReadOnly: true },
  );

  router.delete(
    '/api/terminal/sessions/:id',
    (_req, res, params) => {
      sessions.get(params.id ?? '')?.close();
      sendJson(res, 200, { ok: true });
    },
    { allowInReadOnly: true },
  );
  return {
    dispose: () => {
      for (const session of sessions.values()) session.close();
    },
  };
}
