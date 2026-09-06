import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import type { AuthPolicy } from './auth';
import { checkRequestAuth } from './auth';
import { mapErrorToApiError, stringifyJson } from './json';
import type { Handler } from './router';
import { Router } from './router';

export interface ApiError {
  readonly code: string;
  readonly message: string;
  readonly details?: unknown;
}

/** Sends a JSON body with the given status. Bigint-safe — Kafka offsets are `bigint` everywhere. */
export function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = stringifyJson(body);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(payload);
}

/** The one error shape every `/api/*` route uses on failure: `{ error: { code, message, details? } }`. */
export function sendError(res: ServerResponse, status: number, code: string, message: string, details?: unknown): void {
  const error: ApiError = details === undefined ? { code, message } : { code, message, details };
  sendJson(res, status, { error });
}

export interface CreateServerOptions {
  readonly router: Router;
  /**
   * Fallback for anything the router didn't match — serves the built SPA (or, in dev mode, proxies
   * through Vite). Returning `false` means "I didn't handle this either", which becomes a 404.
   */
  readonly fallback?: (req: IncomingMessage, res: ServerResponse, url: URL) => boolean | Promise<boolean>;
  /** Session-token + origin/host allowlist policy. Omitted entirely by route-level unit tests that talk to the router directly. */
  readonly auth?: AuthPolicy;
  /** Rejects a matched mutating route with `403`, unless the route itself opted out via `allowInReadOnly`. */
  readonly readOnly?: boolean;
}

async function handleRequest(req: IncomingMessage, res: ServerResponse, options: CreateServerOptions): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://studio.internal');

  try {
    if (options.auth) {
      const failure = checkRequestAuth(req, url, options.auth);
      if (failure) {
        sendError(res, failure.status, failure.code, failure.message);
        return;
      }
    }

    const match = options.router.match(req.method ?? 'GET', url.pathname);
    if (match) {
      if (options.readOnly === true && match.mutating && !match.allowInReadOnly) {
        sendError(res, 403, 'read_only', 'the server is running in read-only mode');
        return;
      }
      await match.handler(req, res, match.params, url);
      return;
    }

    if (options.fallback && (await options.fallback(req, res, url))) return;

    if (url.pathname.startsWith('/api/')) {
      sendError(res, 404, 'not_found', `no route for ${req.method ?? 'GET'} ${url.pathname}`);
    } else {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('not found');
    }
  } catch (error) {
    if (res.headersSent) {
      res.end();
      return;
    }
    const mapped = mapErrorToApiError(error);
    sendError(res, mapped.status, mapped.code, mapped.message, mapped.details);
  }
}

export function createStudioServer(options: CreateServerOptions): http.Server {
  return http.createServer((req, res) => {
    void handleRequest(req, res, options);
  });
}

export type { Handler };
