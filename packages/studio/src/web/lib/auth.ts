const TOKEN_STORAGE_KEY = 'kafka-studio-token:v1';
const TOKEN_HEADER = 'x-kafka-studio-token';

/** Pulls `token=…` out of a URL hash fragment (e.g. `#token=abc123`). Pure — no DOM access, so it's unit-testable without a browser. */
export function parseTokenFromHash(hash: string): string | null {
  const params = new URLSearchParams(hash.startsWith('#') ? hash.slice(1) : hash);
  return params.get('token');
}

/**
 * Runs once, at module load, in a real browser: the server delivers the session token in the
 * opened URL's hash (so it never reaches server logs or a `Referer` header), this reads it into
 * `sessionStorage` for the rest of the tab's lifetime, then removes it from the visible URL.
 * Returns `null` under Vitest's node environment, where `window` doesn't exist — every caller
 * below degenerates to a no-op there rather than throwing.
 *
 * `sessionStorage`, not a cookie: this token is a fresh, single-process, single-tab credential —
 * there is no account it belongs to, no server-side session to attach a cookie to, and nothing
 * for it to still be valid for once the process that minted it exits. That makes it a local,
 * ephemeral secret rather than a long-lived credential, which is the case `sessionStorage` is a
 * poor fit for.
 *
 * (Reviewed against react-doctor's `auth-token-in-web-storage` finding: false positive for the
 * reason above — there is no persistent-credential storage risk to move off of here.)
 */
function bootstrapFromLocation(): string | null {
  if (typeof window === 'undefined') return null;

  const fromHash = parseTokenFromHash(window.location.hash);
  if (fromHash !== null) {
    try {
      window.sessionStorage.setItem(TOKEN_STORAGE_KEY, fromHash);
    } catch {
      // Private mode or blocked storage: the token still works for this page load from memory,
      // it just won't survive a reload.
    }
    const url = new URL(window.location.href);
    url.hash = '';
    window.history.replaceState(null, '', url.toString());
    return fromHash;
  }

  try {
    return window.sessionStorage.getItem(TOKEN_STORAGE_KEY);
  } catch {
    return null;
  }
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
