import { useEffect } from 'react';
import { Menu, PanelLeft, RefreshCw } from 'lucide-react';
import { useIsFetching, useQueryClient } from '@tanstack/react-query';
import { Link, useLocation } from '@tanstack/react-router';
import { Button } from '../ui/button';
import { Separator } from '../ui/separator';
import { cn } from '../../lib/utils';
import { useAppShell } from './app-shell';

interface Crumb {
  readonly label: string;
  readonly to?: '/topics' | '/messages';
  /**
   * What the label *is*, when the label alone is just a name. A bare `orders.created` as the page
   * heading says nothing about what you are looking at; `Topic orders.created` does.
   */
  readonly kind?: string;
}

/** `title: null` means no specific page — falls back to the bare app title, no third segment. */
function getPageInfo(pathname: string): { readonly title: string | null; readonly crumbs: readonly Crumb[] } {
  if (pathname === '/') return { title: 'Overview', crumbs: [{ label: 'Cluster' }, { label: 'Overview' }] };
  if (pathname === '/topics') return { title: 'Topics', crumbs: [{ label: 'Topics' }] };
  if (pathname === '/producer') return { title: 'Producer', crumbs: [{ label: 'Producer' }] };
  if (pathname === '/messages') return { title: 'Messages', crumbs: [{ label: 'Messages' }] };
  if (pathname.startsWith('/topics/')) {
    const name = decodeURIComponent(pathname.slice('/topics/'.length));
    return {
      title: name,
      crumbs: [
        { label: 'Topics', to: '/topics' },
        { label: name, kind: 'Topic' },
      ],
    };
  }
  return { title: null, crumbs: [] };
}

/**
 * Sidebar toggle, the breadcrumb trail (which doubles as the page heading), and refresh. There is
 * deliberately no global search box: each list filters itself in its own toolbar, and a second
 * search field in the chrome would just be a jump-by-exact-name shortcut with nowhere to go.
 */
export function Topbar() {
  const { isMobile, collapsed, toggleCollapsed, openMobileNav } = useAppShell();
  const pathname = useLocation({ select: (location) => location.pathname });
  const queryClient = useQueryClient();
  const fetching = useIsFetching();
  const { title, crumbs } = getPageInfo(pathname);
  const lastCrumbIndex = crumbs.length - 1;

  useEffect(() => {
    // Mirrors the docs site's `Kafka | <package> | <title>` convention (`BaseLayout.astro`).
    document.title = title === null ? 'Kafka | Studio' : `Kafka | Studio | ${title}`;
  }, [title]);

  return (
    <header className="sticky top-0 z-30 flex h-16 shrink-0 items-center justify-between gap-3 border-b border-border bg-background/80 px-4 backdrop-blur lg:px-6">
      <div className="flex min-w-0 items-center gap-2">
        {isMobile ? (
          <Button
            type="button"
            variant="ghost"
            size="icon-lg"
            onClick={openMobileNav}
            aria-label="Open navigation menu"
          >
            <Menu className="size-5" aria-hidden="true" />
          </Button>
        ) : (
          <Button
            type="button"
            variant="ghost"
            size="icon-lg"
            onClick={toggleCollapsed}
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            aria-pressed={collapsed}
          >
            <PanelLeft className="size-5" aria-hidden="true" />
          </Button>
        )}
        <Separator orientation="vertical" className="h-6" />
        {/*
          The trail is the page heading — the last crumb *is* the `h1`, so the title, the
          breadcrumb and `document.title` all come from one place and nothing is repeated.
        */}
        <nav aria-label="Breadcrumb" className="ml-1 min-w-0">
          <ol className="flex min-w-0 items-center gap-2 text-lg">
            {crumbs.map((crumb, index) => (
              <li key={crumb.label} className="flex min-w-0 items-center gap-2">
                {index > 0 && (
                  <span aria-hidden="true" className="text-muted-foreground/60">
                    /
                  </span>
                )}
                {index === lastCrumbIndex ? (
                  <h1 className="flex min-w-0 items-baseline gap-2" aria-current="page">
                    {crumb.kind !== undefined && <span className="shrink-0 text-muted-foreground">{crumb.kind}</span>}
                    <span className="truncate font-semibold">{crumb.label}</span>
                  </h1>
                ) : crumb.to === undefined ? (
                  <span className="truncate text-muted-foreground">{crumb.label}</span>
                ) : (
                  <Link
                    to={crumb.to}
                    className="truncate rounded-sm text-muted-foreground outline-none hover:text-foreground hover:underline focus-visible:ring-3 focus-visible:ring-ring/50"
                  >
                    {crumb.label}
                  </Link>
                )}
              </li>
            ))}
          </ol>
        </nav>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <Button
          type="button"
          variant="outline"
          size="lg"
          onClick={() => void queryClient.invalidateQueries()}
          aria-label="Refresh data"
        >
          <RefreshCw className={cn('size-4', fetching > 0 && 'animate-spin')} aria-hidden="true" />
          <span className="max-sm:sr-only">Refresh</span>
        </Button>
      </div>
    </header>
  );
}
