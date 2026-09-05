import { BookOpen } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { cn } from '../../lib/utils';
import { useAppShell } from './app-shell';
import { ConnectionStatus } from './connection-status';
import { ProfileSwitcher } from './profile-switcher';
import { SidebarNav } from './sidebar-nav';

const DOCS_URL = 'https://cookiemonsterdev.github.io/kafka/';

export interface SidebarContentProps {
  readonly collapsed: boolean;
  /** Called after a real navigation — used by the mobile drawer to close itself. */
  readonly onNavigate?: () => void;
}

/** The accent-tiled brand mark. Shared by the sidebar header and the mobile drawer header. */
export function BrandMark({ className }: { readonly className?: string }) {
  return (
    <span
      className={cn(
        'flex size-9 shrink-0 items-center justify-center rounded-lg border border-primary/25 bg-primary/12',
        className,
      )}
    >
      <img src="/logo-mark.svg" alt="" width="22" height="22" className="size-5.5" />
    </span>
  );
}

function DocumentationLink({ collapsed, onNavigate }: SidebarContentProps) {
  const link = (
    <a
      href={DOCS_URL}
      rel="noopener noreferrer"
      target="_blank"
      onClick={onNavigate}
      className={cn(
        'flex h-10 items-center gap-3 rounded-lg px-3 text-sm text-muted-foreground outline-none transition-colors',
        'hover:bg-muted/50 hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50',
        collapsed && 'justify-center px-0',
      )}
    >
      <BookOpen className="size-4.5 shrink-0" aria-hidden="true" />
      <span className={collapsed ? 'sr-only' : 'truncate'}>Documentation</span>
    </a>
  );

  if (!collapsed) return link;
  return (
    <Tooltip>
      <TooltipTrigger asChild>{link}</TooltipTrigger>
      <TooltipContent side="right">Documentation</TooltipContent>
    </Tooltip>
  );
}

/**
 * The workspace nav, environment switcher, and connection card — shared verbatim between the
 * fixed sidebar and the mobile drawer (`MobileNavSheet`) so the two chrome states can never drift
 * apart or leave a control missing on one of them.
 */
export function SidebarContent({ collapsed, onNavigate }: SidebarContentProps) {
  return (
    <>
      <div className="min-h-0 flex-1 overflow-y-auto p-2">
        <SidebarNav collapsed={collapsed} onNavigate={onNavigate} />
        {/* Renders nothing at all when no profiles are configured — a section heading with no
            control under it reads as something failing to load. */}
        <ProfileSwitcher collapsed={collapsed} className="mt-6" />
      </div>
      <div className="p-2">
        <ConnectionStatus collapsed={collapsed} />
      </div>
      <div className="p-2 pt-0">
        <DocumentationLink collapsed={collapsed} onNavigate={onNavigate} />
      </div>
    </>
  );
}

/** Fixed desktop/tablet chrome. Renders nothing on mobile — the drawer takes over there instead. */
export function Sidebar() {
  const { isMobile, collapsed } = useAppShell();
  if (isMobile) return null;

  return (
    <nav
      aria-label="Primary"
      className={cn(
        'flex shrink-0 flex-col border-r border-border bg-sidebar transition-[width] duration-150',
        collapsed ? 'w-16' : 'w-64',
      )}
    >
      <div className={cn('flex h-16 shrink-0 items-center gap-2.5 px-3', collapsed && 'justify-center px-0')}>
        <BrandMark />
        {!collapsed && <span className="min-w-0 truncate text-sm font-semibold">Kafka Studio</span>}
      </div>
      {/* The collapse control lives in the top bar (see `Topbar`), where it stays in the same
          place whether the rail is expanded or not. */}
      <SidebarContent collapsed={collapsed} />
    </nav>
  );
}
