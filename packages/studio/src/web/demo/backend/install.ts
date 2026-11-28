import { DemoApi } from './demo-api';
import { createFakeEventSource } from './fake-event-source';

const TRAFFIC_INTERVAL_MS = 1_000;

function isApiRequest(url: URL): boolean {
  return url.origin === window.location.origin && url.pathname.startsWith('/api/');
}

async function readJson(request: Request): Promise<unknown> {
  const text = await request.text();
  if (text === '') return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

/**
 * Answers the studio's `/api/*` calls inside the page: `fetch` and `EventSource` are replaced
 * before the app mounts, so nothing leaves the browser. Other requests (the app's own assets) pass
 * through. Background traffic runs only while the page is visible.
 */
export function installDemoBackend(): void {
  const api = new DemoApi();
  const realFetch = window.fetch.bind(window);
  window.fetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (!isApiRequest(url)) return realFetch(input, init);
    request.signal.throwIfAborted();
    return api.handle(request.method, url, await readJson(request));
  };
  window.EventSource = createFakeEventSource(api.openStream, window.location.href);

  setInterval(() => {
    if (document.visibilityState === 'visible') api.cluster.tick();
  }, TRAFFIC_INTERVAL_MS);
}
