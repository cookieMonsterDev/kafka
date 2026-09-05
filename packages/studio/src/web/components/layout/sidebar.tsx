import { PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import { Button } from '../ui/button';
import { cn } from '../../lib/utils';
import { useAppShell } from './app-shell';
import { ConnectionStatus } from './connection-status';
import { ProfileSwitcher } from './profile-switcher';
import { SidebarNav } from './sidebar-nav';

export interface SidebarContentProps {
  readonly collapsed: boolean;
  /** Called after a real navigation — used by the mobile drawer to close itself. */
  readonly onNavigate?: () => void;
}

/**
 * The workspace nav, profile switcher, and connection pill — shared verbatim between the fixed
 * sidebar and the mobile drawer (`MobileNavSheet`) so the two chrome states can never drift apart
 * or leave a control missing on one of them.
 */
export function SidebarContent({ collapsed, onNavigate }: SidebarContentProps) {
  return (
    <>
      <div className="flex-1 overflow-y-auto p-2">
        <SidebarNav collapsed={collapsed} onNavigate={onNavigate} />
      </div>
      <div className="border-t border-border p-2">
        <ProfileSwitcher collapsed={collapsed} />
      </div>
      <div className="border-t border-border p-2">
        <ConnectionStatus collapsed={collapsed} />
      </div>
    </>
  );
}

/** Fixed desktop/tablet chrome. Renders nothing on mobile — the drawer takes over there instead. */
export function Sidebar() {
  const { isMobile, collapsed, toggleCollapsed } = useAppShell();
  if (isMobile) return null;

  return (
    <nav
      aria-label="Primary"
      className={cn(
        'flex shrink-0 flex-col border-r border-border bg-card transition-[width] duration-150',
        collapsed ? 'w-16' : 'w-60',
      )}
    >
      <div className="flex h-14 items-center border-b border-border px-3">
        <span className={cn('truncate text-sm font-semibold', collapsed && 'sr-only')}>Kafka Studio</span>
      </div>
      <SidebarContent collapsed={collapsed} />
      <div className="border-t border-border p-2">
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="w-full"
          onClick={toggleCollapsed}
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          aria-pressed={collapsed}
        >
          {collapsed ? (
            <PanelLeftOpen className="size-4" aria-hidden="true" />
          ) : (
            <PanelLeftClose className="size-4" aria-hidden="true" />
          )}
        </Button>
      </div>
    </nav>
  );
}
