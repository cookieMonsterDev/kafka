import { useEffect, useState, type FormEvent } from 'react';
import { Menu, RefreshCw } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { Link, useLocation, useNavigate } from '@tanstack/react-router';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { ThemeToggle } from './theme-toggle';
import { useAppShell } from './app-shell';

interface Crumb {
  readonly label: string;
  readonly to?: '/topics';
}

function getPageInfo(pathname: string): { readonly title: string; readonly crumbs: readonly Crumb[] } {
  if (pathname === '/') return { title: 'Cluster overview', crumbs: [{ label: 'Cluster' }] };
  if (pathname === '/topics') return { title: 'Topics', crumbs: [{ label: 'Topics' }] };
  if (pathname.startsWith('/topics/')) {
    const name = decodeURIComponent(pathname.slice('/topics/'.length));
    return {
      title: name,
      crumbs: [{ label: 'Topics', to: '/topics' }, { label: name }],
    };
  }
  return { title: 'Kafka Studio', crumbs: [] };
}

/** Breadcrumb + page title, a topic-jump search box, and a refresh action. */
export function Topbar() {
  const { isMobile, openMobileNav } = useAppShell();
  const pathname = useLocation({ select: (location) => location.pathname });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [search, setSearch] = useState('');
  const { title, crumbs } = getPageInfo(pathname);
  const lastCrumbIndex = crumbs.length - 1;

  useEffect(() => {
    document.title = title === 'Kafka Studio' ? title : `${title} · Kafka Studio`;
  }, [title]);

  function handleSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const name = search.trim();
    if (name === '') return;
    setSearch('');
    void navigate({ to: '/topics/$name', params: { name } });
  }

  return (
    <header className="flex items-center justify-between gap-3 border-b border-border px-4 py-3">
      <div className="flex min-w-0 items-center gap-2">
        {isMobile && (
          <Button type="button" variant="ghost" size="icon" onClick={openMobileNav} aria-label="Open navigation menu">
            <Menu className="size-5" aria-hidden="true" />
          </Button>
        )}
        <nav aria-label="Breadcrumb" className="min-w-0">
          <ol className="flex min-w-0 items-center gap-1.5 text-sm text-muted-foreground">
            {crumbs.map((crumb, index) => (
              <li key={crumb.label} className="flex min-w-0 items-center gap-1.5">
                {index > 0 && <span aria-hidden="true">/</span>}
                {crumb.to === undefined || index === lastCrumbIndex ? (
                  <span
                    className={index === lastCrumbIndex ? 'truncate font-medium text-foreground' : 'truncate'}
                    aria-current={index === lastCrumbIndex ? 'page' : undefined}
                  >
                    {crumb.label}
                  </span>
                ) : (
                  <Link
                    to={crumb.to}
                    className="truncate rounded-sm outline-none hover:underline focus-visible:ring-3 focus-visible:ring-ring/50"
                  >
                    {crumb.label}
                  </Link>
                )}
              </li>
            ))}
          </ol>
        </nav>
      </div>
      <div className="flex items-center gap-2">
        <form onSubmit={handleSearch}>
          <label className="sr-only" htmlFor="topbar-search">
            Jump to topic
          </label>
          <Input
            id="topbar-search"
            type="search"
            placeholder="Jump to topic…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            className="h-8 w-24 sm:w-44"
          />
        </form>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          aria-label="Refresh data"
          onClick={() => void queryClient.invalidateQueries()}
        >
          <RefreshCw className="size-4" aria-hidden="true" />
        </Button>
        <ThemeToggle />
      </div>
    </header>
  );
}
