const TOKEN_HEADER = 'x-kafka-studio-token';

/** Pulls `token=…` out of a URL hash fragment (e.g. `#token=abc123`). Pure — no DOM access, so it's unit-testable without a browser. */
export function parseTokenFromHash(hash: string): string | null {
  const params = new URLSearchParams(hash.startsWith('#') ? hash.slice(1) : hash);
  return params.get('token');
}

/**
 * Runs once, at module load, in a real browser: the server delivers the session token in the
 * opened URL's hash (so it never reaches server logs or a `Referer` header); this reads it into
 * the in-memory `sessionToken` module variable below, then removes it from the visible URL.
 * Returns `null` under Vitest's node environment, where `window` doesn't exist — every caller
 * below degenerates to a no-op there rather than throwing.
 *
 * In memory only, never `sessionStorage`: a script that ran via XSS can read anything in Web
 * Storage for as long as the tab stays open, whereas a value that lives only in this module's
 * closure is reachable solely by code that already runs in this same scope. The cost is a hard
 * reload loses the session — the studio's own terminal banner is how you get back in, printing
 * the same URL (and the same token, valid for the whole life of that server process) again.
 */
function bootstrapFromLocation(): string | null {
  if (typeof window === 'undefined') return null;

  const fromHash = parseTokenFromHash(window.location.hash);
  if (fromHash === null) return null;

  const url = new URL(window.location.href);
  url.hash = '';
  window.history.replaceState(null, '', url.toString());
  return fromHash;
}

const sessionToken = bootstrapFromLocation();

export function getSessionToken(): string | null {
  return sessionToken;
}

/** Adds the session token header to a `fetch` init, if one was ever delivered — a missing/invalid token surfaces as the same 401 `assertOk` already maps every other API failure to. */
export function withAuthHeaders(init?: HeadersInit): Headers {
  const headers = new Headers(init);
  if (sessionToken !== null) headers.set(TOKEN_HEADER, sessionToken);
  return headers;
}

/** `EventSource` cannot set request headers, so live streams carry the token as a query param instead. */
export function withAuthQuery(url: string): string {
  if (sessionToken === null) return url;
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}token=${encodeURIComponent(sessionToken)}`;
}
