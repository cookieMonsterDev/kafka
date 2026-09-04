import { Outlet, createRootRoute } from '@tanstack/react-router';
import { ThemeToggle } from '../components/layout/theme-toggle';

export const rootRoute = createRootRoute({ component: RootLayout });

function RootLayout() {
  return (
    <div className="flex min-h-dvh flex-col">
      <a
        href="#main-content"
        className="sr-only focus-visible:not-sr-only focus-visible:absolute focus-visible:top-2 focus-visible:left-2 focus-visible:z-50 focus-visible:rounded-lg focus-visible:bg-background focus-visible:px-3 focus-visible:py-2 focus-visible:text-sm"
      >
        Skip to content
      </a>
      <header className="flex items-center justify-between border-b border-border px-4 py-3">
        <span className="text-sm font-medium">Kafka Studio</span>
        <ThemeToggle />
      </header>
      <main id="main-content" className="flex-1 p-4">
        <Outlet />
      </main>
    </div>
  );
}
