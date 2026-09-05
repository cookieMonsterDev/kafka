import { Layers, MessageSquare, Send, Server, Settings, Users, Workflow, type LucideIcon } from 'lucide-react';
import { Link } from '@tanstack/react-router';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { cn } from '../../lib/utils';

interface NavItem {
  readonly label: string;
  readonly icon: LucideIcon;
  /** Omitted for workspace areas that don't have a route yet — rendered disabled, never a dead link. */
  readonly to?: '/' | '/topics';
  readonly exact?: boolean;
}

const NAV_ITEMS: readonly NavItem[] = [
  { label: 'Cluster', icon: Server, to: '/', exact: true },
  { label: 'Topics', icon: Layers, to: '/topics' },
  { label: 'Producer', icon: Send },
  { label: 'Messages', icon: MessageSquare },
  { label: 'Board', icon: Workflow },
  { label: 'Consumer groups', icon: Users },
  { label: 'Settings', icon: Settings },
];

const itemClassName =
  'flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-sm text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50 [&.active]:bg-muted [&.active]:text-foreground [&.active]:font-medium';

export interface SidebarNavProps {
  /** Icon-only rendering: labels move to a tooltip and an `sr-only` accessible name. */
  readonly collapsed: boolean;
  /** Called after a real navigation — used by the mobile drawer to close itself. */
  readonly onNavigate?: () => void;
}

function NavItemContent({ item, collapsed }: { readonly item: NavItem; readonly collapsed: boolean }) {
  const Icon = item.icon;
  return (
    <>
      <Icon className="size-4 shrink-0" aria-hidden="true" />
      <span className={collapsed ? 'sr-only' : undefined}>{item.label}</span>
      {!collapsed && item.to === undefined && (
        <span className="ml-auto rounded-full border border-border px-1.5 py-0.5 text-[0.65rem] text-muted-foreground">
          Soon
        </span>
      )}
    </>
  );
}

function NavItem({ item, collapsed, onNavigate }: { readonly item: NavItem } & SidebarNavProps) {
  const content =
    item.to === undefined ? (
      <button type="button" disabled className={cn(itemClassName, 'w-full cursor-not-allowed opacity-60')}>
        <NavItemContent item={item} collapsed={collapsed} />
      </button>
    ) : (
      <Link
        to={item.to}
        activeOptions={item.exact === true ? { exact: true } : undefined}
        activeProps={{ className: 'active' }}
        className={itemClassName}
        onClick={onNavigate}
      >
        <NavItemContent item={item} collapsed={collapsed} />
      </Link>
    );

  if (!collapsed) return content;

  return (
    <Tooltip>
      <TooltipTrigger asChild>{content}</TooltipTrigger>
      <TooltipContent side="right">{item.to === undefined ? `${item.label} — coming soon` : item.label}</TooltipContent>
    </Tooltip>
  );
}

/** Shared between the fixed sidebar and the mobile drawer so the two never drift apart. */
export function SidebarNav({ collapsed, onNavigate }: SidebarNavProps) {
  return (
    <ul className="flex flex-col gap-1">
      {NAV_ITEMS.map((item) => (
        <li key={item.label}>
          <NavItem item={item} collapsed={collapsed} onNavigate={onNavigate} />
        </li>
      ))}
    </ul>
  );
}
