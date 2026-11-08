import { FileQuestion } from 'lucide-react';
import { Link, Outlet, createRootRoute } from '@tanstack/react-router';
import { TerminalProvider } from '../components/terminal/terminal-context';
import { AppShell } from '../components/layout/app-shell';
import { Button } from '../components/ui/button';
import { EmptyState } from '../components/ui/empty-state';
import { ErrorState } from '../components/ui/error-state';

export const rootRoute = createRootRoute({
  component: RootLayout,
  // A render crash inside a route would otherwise take the whole SPA down to a blank page. The
  // shell stays mounted, so the sidebar and top bar remain usable to navigate away.
  errorComponent: PageError,
  notFoundComponent: RouteNotFound,
});

export function PageError({ error, reset }: { readonly error: unknown; readonly reset: () => void }) {
  return (
    <ErrorState
      className="min-h-0 flex-1 justify-center p-6"
      title="This page failed to render"
      error={error}
      onRetry={reset}
      retryLabel="Reload the page"
    />
  );
}

function RouteNotFound() {
  return (
    <EmptyState
      className="min-h-0 flex-1 justify-center p-6"
      icon={FileQuestion}
      title="Page not found"
      description="That route does not exist in the studio."
      action={
        <Button asChild variant="outline" size="sm">
          <Link to="/">Back to the cluster overview</Link>
        </Button>
      }
    />
  );
}

function RootLayout() {
  return (
    <TerminalProvider>
      <AppShell>
        <Outlet />
      </AppShell>
    </TerminalProvider>
  );
}
