import { randomBytes, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage } from 'node:http';

export const SESSION_TOKEN_HEADER = 'x-kafka-studio-token';
const SESSION_TOKEN_QUERY_PARAM = 'token';

/** Hostnames that all mean "this machine" for the purposes of the origin/host allowlist below. */
const LOOPBACK_HOSTNAMES: ReadonlySet<string> = new Set(['localhost', '127.0.0.1', '::1']);

/** Bind addresses that accept connections from more than just this machine. */
const BIND_ALL_HOSTS: ReadonlySet<string> = new Set(['0.0.0.0', '::']);

/** A fresh, per-process secret. Delivered to the browser once, in the opened URL's hash — never as a query string or header the server itself would log. */
export function generateSessionToken(): string {
  return randomBytes(32).toString('hex');
}

export function isLoopbackHost(host: string): boolean {
  return LOOPBACK_HOSTNAMES.has(host);
}

function constantTimeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a, 'utf8');
  const bufB = Buffer.from(b, 'utf8');
  // Deliberately not an early `return false` on length mismatch before touching `timingSafeEqual`
  // — that branch alone doesn't leak useful timing (token lengths are fixed), and `timingSafeEqual`
  // requires equal-length buffers, so the guard is required either way.
  if (bufA.length !== bufB.length) return false;
  return timingSafeEqual(bufA, bufB);
}

function normalizeHostname(hostname: string): string {
  return hostname.startsWith('[') && hostname.endsWith(']') ? hostname.slice(1, -1) : hostname;
}

/** Splits a `Host` header (`example.com:5757`, or the bracketed `[::1]:5757`) into hostname and port. */
function splitHostHeader(value: string): { readonly hostname: string; readonly port: string | undefined } {
  const bracketed = /^\[([^\]]+)](?::(\d+))?$/.exec(value);
  if (bracketed) return { hostname: bracketed[1] ?? '', port: bracketed[2] };

  const lastColon = value.lastIndexOf(':');
  if (lastColon === -1) return { hostname: value, port: undefined };
  return { hostname: value.slice(0, lastColon), port: value.slice(lastColon + 1) };
}

export interface OriginPolicy {
  /** The literal address the server was told to bind — `127.0.0.1` by default, or an operator-chosen `--host`. */
  readonly host: string;
  readonly port: number;
}

function hostnameAllowed(hostname: string, policy: OriginPolicy): boolean {
  if (BIND_ALL_HOSTS.has(policy.host)) return true;
  const normalized = normalizeHostname(hostname);
  if (normalized === normalizeHostname(policy.host)) return true;
  return isLoopbackHost(policy.host) && isLoopbackHost(normalized);
}

/**
 * `Host` is a mandatory header on every HTTP/1.1+ request, so unlike `Origin` there is no "absent"
 * case to tolerate — a request naming a hostname this server wasn't told to answer for is exactly
 * the DNS-rebinding shape this check exists to catch.
 */
export function isAllowedHostHeader(hostHeader: string | undefined, policy: OriginPolicy): boolean {
  if (hostHeader === undefined || hostHeader.length === 0) return false;
  const { hostname, port } = splitHostHeader(hostHeader);
  if (!hostnameAllowed(hostname, policy)) return false;
  return Number(port ?? '80') === policy.port;
}

/**
 * Browsers omit `Origin` on plain same-origin navigations and on many same-origin `fetch` calls,
 * so its absence is not itself suspicious — `isAllowedHostHeader` is what actually guards against a
 * hostile origin, since `Host` is never omitted. This only rejects an `Origin` that is *present*
 * and wrong.
 */
export function isAllowedOriginHeader(originHeader: string | undefined, policy: OriginPolicy): boolean {
  if (originHeader === undefined || originHeader.length === 0) return true;

  let origin: URL;
  try {
    origin = new URL(originHeader);
  } catch {
    return false;
  }
  if (origin.protocol !== 'http:') return false;
  if (!hostnameAllowed(origin.hostname, policy)) return false;

  const port = origin.port === '' ? 80 : Number(origin.port);
  return port === policy.port;
}

/** The token a request presented — the header for ordinary `fetch` calls, or the query param `EventSource` falls back to since it cannot set custom headers. */
export function readPresentedToken(req: Pick<IncomingMessage, 'headers'>, url: URL): string | null {
  const header = req.headers[SESSION_TOKEN_HEADER];
  if (typeof header === 'string' && header.length > 0) return header;

  const fromQuery = url.searchParams.get(SESSION_TOKEN_QUERY_PARAM);
  return fromQuery !== null && fromQuery.length > 0 ? fromQuery : null;
}

export function isValidToken(req: Pick<IncomingMessage, 'headers'>, url: URL, expectedToken: string): boolean {
  const presented = readPresentedToken(req, url);
  return presented !== null && constantTimeEqual(presented, expectedToken);
}

export interface AuthFailure {
  readonly status: number;
  readonly code: string;
  readonly message: string;
}

export interface AuthPolicy extends OriginPolicy {
  readonly token: string;
}

/** `/api/*` and the runtime bootstrap file are the only routes carrying anything worth protecting — built assets need no auth to load the page that reads the token out of the URL in the first place. */
export function requiresAuth(pathname: string): boolean {
  return pathname.startsWith('/api/') || pathname === '/__studio_runtime.json';
}

/**
 * The one place both non-optional local-server controls from the security model come together:
 * an origin/host allowlist (DNS-rebinding defense) and a per-process session token. Returns `null`
 * when the request may proceed.
 */
export function checkRequestAuth(
  req: Pick<IncomingMessage, 'headers'>,
  url: URL,
  policy: AuthPolicy,
): AuthFailure | null {
  if (!requiresAuth(url.pathname)) return null;

  if (!isAllowedOriginHeader(req.headers.origin, policy) || !isAllowedHostHeader(req.headers.host, policy)) {
    return { status: 403, code: 'forbidden_origin', message: 'request origin or host is not allowed' };
  }
  if (!isValidToken(req, url, policy.token)) {
    return { status: 401, code: 'unauthorized', message: 'missing or invalid session token' };
  }
  return null;
}

/** Appended to the opened URL's hash — never a query string, so it never reaches server access logs or a `Referer` header. */
export function withSessionTokenHash(url: string, token: string): string {
  return `${url}#token=${encodeURIComponent(token)}`;
}

/**
 * A loud, explicit warning for the one flag that turns off the localhost-only default — printed,
 * never silently accepted, because this server can drive a possibly-production Kafka cluster.
 */
export function hostSecurityWarning(host: string): string | null {
  if (isLoopbackHost(host)) return null;
  return [
    `WARNING: binding to ${host} exposes this server — and whatever Kafka cluster it is`,
    'connected to — to every other host that can reach this machine on the network.',
    'Only do this on a network you trust.',
  ].join(' ');
}
