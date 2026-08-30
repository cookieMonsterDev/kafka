import { useRef, useState, type ReactNode } from 'react';
import { PackageSwitcherSelect } from '@/components/package-switcher-select';
import { isCurrentPath, type DocsNavPackage } from '@/lib/docs';
import { cn } from '@/lib/utils';

type DocsMobileNavProps = {
  packages: DocsNavPackage[];
  currentPackage: string;
  currentPath: string;
  children?: ReactNode;
};

export function DocsMobileNav({ packages, currentPackage, currentPath, children }: DocsMobileNavProps) {
  const paneRef = useRef<HTMLDivElement>(null);
  const [selected, setSelected] = useState(currentPackage);
  const [status, setStatus] = useState('');
  const active = packages.find((pkg) => pkg.id === selected) ?? packages[0];

  if (active == null) {
    return null;
  }

  function onPackageChange(next: string) {
    const pkg = packages.find((item) => item.id === next);
    if (pkg == null || pkg.id === selected) {
      return;
    }
    setSelected(pkg.id);
    setStatus(`Showing ${pkg.label} documentation`);
    paneRef.current?.scrollTo({ top: 0 });
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
      <PackageSwitcherSelect
        className="mb-6 shrink-0"
        packages={packages}
        value={selected}
        onPackageChange={onPackageChange}
      />
      {children}
      <div ref={paneRef} className="docs-nav-pane pb-2">
        <nav key={active.id} className="text-sm" aria-label="Documentation">
          {active.groups.map((group) => (
            <details key={group.section} open className="group/section mb-2">
              <summary className="text-muted-foreground hover:text-foreground flex cursor-pointer list-none items-center justify-between gap-2 rounded-md px-2 py-2 text-xs font-medium tracking-wide uppercase [&::-webkit-details-marker]:hidden">
                {group.label}
                <svg
                  className="size-3.5 shrink-0 transition-transform group-open/section:rotate-180"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  aria-hidden="true"
                >
                  <path d="m6 9 6 6 6-6"></path>
                </svg>
              </summary>
              <ul className="flex flex-col gap-0.5 pb-3">
                {group.entries.map((entry) => {
                  const current = isCurrentPath(entry.href, currentPath);
                  return (
                    <li key={entry.href}>
                      <a
                        href={entry.href}
                        aria-current={current ? 'page' : undefined}
                        className={cn(
                          'flex min-h-10 items-center rounded-md px-2 py-2 transition-colors',
                          current
                            ? 'bg-sidebar-accent text-sidebar-accent-foreground font-medium'
                            : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground',
                        )}
                      >
                        {entry.label}
                      </a>
                    </li>
                  );
                })}
              </ul>
            </details>
          ))}
        </nav>
      </div>
      <span className="sr-only" aria-live="polite">
        {status}
      </span>
    </div>
  );
}
