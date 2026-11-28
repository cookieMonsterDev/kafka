import { createMemoryHistory } from '@tanstack/react-router';
import { mountStudio } from '../mount';
import { createStudioRouter } from '../router';
import { installDemoBackend } from './backend/install';

const CHUNK_RELOAD_KEY = 'kafka-studio-demo:chunk-reload';
const CHUNK_RELOAD_INTERVAL_MS = 10_000;

// A redeploy replaces the hashed route chunks, so a frame opened before it fails to load the next
// screen it opens. Reloading picks up the new build; the interval stops a broken deploy from looping.
window.addEventListener('vite:preloadError', (event) => {
  try {
    const lastReload = Number(sessionStorage.getItem(CHUNK_RELOAD_KEY) ?? 0);
    if (Date.now() - lastReload < CHUNK_RELOAD_INTERVAL_MS) return;
    sessionStorage.setItem(CHUNK_RELOAD_KEY, String(Date.now()));
  } catch {
    return;
  }
  event.preventDefault();
  window.location.reload();
});

installDemoBackend();
// Memory history keeps navigation inside the app, so the demo never rewrites its frame's URL.
mountStudio(createStudioRouter(createMemoryHistory({ initialEntries: ['/'] })));
