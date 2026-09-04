import { Link, Outlet, createRootRoute } from '@tanstack/react-router';
import { ProfileSwitcher } from '../components/layout/profile-switcher';
import { ThemeToggle } from '../components/layout/theme-toggle';

export const rootRoute = createRootRoute({ component: RootLayout });

const navLinkClassName =
  'rounded-lg px-2.5 py-1.5 text-sm text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50 [&.active]:bg-muted [&.active]:text-foreground [&.active]:font-medium';

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
        <div className="flex items-center gap-4">
          <span className="text-sm font-medium">Kafka Studio</span>
          <nav aria-label="Primary" className="flex items-center gap-1">
            <Link
              to="/"
              className={navLinkClassName}
              activeOptions={{ exact: true }}
              activeProps={{ className: 'active' }}
            >
              Cluster
            </Link>
            <Link to="/topics" className={navLinkClassName} activeProps={{ className: 'active' }}>
              Topics
            </Link>
          </nav>
        </div>
        <div className="flex items-center gap-3">
          <ProfileSwitcher />
          <ThemeToggle />
        </div>
      </header>
      <main id="main-content" className="flex-1 p-4">
        <Outlet />
      </main>
    </div>
  );
}
