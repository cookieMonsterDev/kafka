import { defineConfig, type Plugin } from 'vite';
import { spaPlugins } from './vite.web.config';

const WEB_ENTRY = '<script type="module" src="./main.tsx"></script>';
const WEB_TITLE = '<title>Kafka | Studio</title>';

// `lib/auth` reads the session token from the URL hash once, when it first evaluates. The demo has
// no server to issue one, so a classic script (which runs before any module chunk) supplies a
// placeholder the in-page backend never checks; `lib/auth` then strips it from the URL as usual.
const DEMO_SESSION = `<script>
      history.replaceState(null, '', location.pathname + location.search + '#token=demo');
    </script>`;

/** Serves `src/web/index.html` (splash and all) with the demo entry in place of the real one. */
function demoEntry(): Plugin {
  return {
    name: 'studio-demo-entry',
    transformIndexHtml: {
      order: 'pre',
      handler(html) {
        if (!html.includes(WEB_ENTRY) || !html.includes(WEB_TITLE)) {
          throw new Error(
            'vite.demo.config.ts: src/web/index.html changed its entry script or title; update this plugin',
          );
        }
        return html
          .replace(WEB_ENTRY, `${DEMO_SESSION}\n    <script type="module" src="./demo/main.tsx"></script>`)
          .replace(WEB_TITLE, '<title>Kafka | Studio demo</title>\n    <meta name="robots" content="noindex" />');
      },
    },
  };
}

// Builds the studio SPA against an in-page fake cluster (src/web/demo) for the documentation site,
// which embeds it in an iframe. A relative base lets the output live under any docs path.
export default defineConfig({
  root: 'src/web',
  base: './',
  plugins: [...spaPlugins(), demoEntry()],
  build: {
    outDir: '../../dist/demo',
    emptyOutDir: true,
    target: 'es2022',
  },
});
