import { Layers, MessageSquare, Send, Server, Settings, Users, Workflow, type LucideIcon } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { Badge } from '../ui/badge';
import { Skeleton } from '../ui/skeleton';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { listTopics, topicQueryKeys } from '../../lib/topics-api';
import { cn } from '../../lib/utils';

interface NavItem {
  readonly label: string;
  readonly icon: LucideIcon;
  /** Omitted for workspace areas that don't have a route yet — rendered disabled, never a dead link. */
  readonly to?: '/' | '/topics' | '/producer';
  readonly exact?: boolean;
  /** Shows a live count in the trailing badge slot. Only Topics has a count to show today. */
  readonly counted?: boolean;
}

const NAV_ITEMS: readonly NavItem[] = [
  { label: 'Cluster', icon: Server, to: '/', exact: true },
  { label: 'Topics', icon: Layers, to: '/topics', counted: true },
  { label: 'Producer', icon: Send, to: '/producer' },
  { label: 'Messages', icon: MessageSquare },
  { label: 'Board', icon: Workflow },
  { label: 'Consumer groups', icon: Users },
  { label: 'Settings', icon: Settings },
];

/**
 * The active item carries three cues, not one: the accent rail, the raised surface, and the
 * tinted icon. Colour alone would fail the contrast rule in CONTRIBUTING.
 */
const itemClassName = cn(
  'relative flex h-10 items-center gap-3 rounded-lg px-3 text-sm text-muted-foreground outline-none transition-colors',
  'hover:bg-muted/50 hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50',
  '[&.active]:bg-accent [&.active]:font-medium [&.active]:text-foreground',
  '[&.active_svg]:text-primary',
  'before:absolute before:top-1/2 before:left-0 before:h-5 before:w-[3px] before:-translate-y-1/2 before:rounded-r-full before:bg-primary before:opacity-0',
  '[&.active]:before:opacity-100',
);

export interface SidebarNavProps {
  /** Icon-only rendering: labels move to a tooltip and an `sr-only` accessible name. */
  readonly collapsed: boolean;
  /** Called after a real navigation — used by the mobile drawer to close itself. */
  readonly onNavigate?: () => void;
}

function TopicCount() {
  const { data, isPending, isError } = useQuery({ queryKey: topicQueryKeys.list(), queryFn: listTopics });

  if (isPending) return <Skeleton className="ml-auto h-4 w-7 rounded-full" />;
  if (isError) return null;
  return (
    <Badge variant="outline" className="ml-auto">
      {data.topics.length}
    </Badge>
  );
}

function NavItemContent({ item, collapsed }: { readonly item: NavItem; readonly collapsed: boolean }) {
  const Icon = item.icon;
  return (
    <>
      <Icon className="size-4.5 shrink-0" aria-hidden="true" />
      <span className={collapsed ? 'sr-only' : 'truncate'}>{item.label}</span>
      {!collapsed && item.to === undefined && (
        <Badge variant="outline" className="ml-auto">
          Soon
        </Badge>
      )}
      {!collapsed && item.counted === true && <TopicCount />}
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
        className={cn(itemClassName, collapsed && 'justify-center px-0')}
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
    <ul className="flex flex-col gap-0.5">
      {!collapsed && (
        <li className="px-3 pt-1 pb-1.5 text-[0.6875rem] font-medium tracking-widest text-muted-foreground uppercase">
          Workspace
        </li>
      )}
      {NAV_ITEMS.map((item) => (
        <li key={item.label}>
          <NavItem item={item} collapsed={collapsed} onNavigate={onNavigate} />
        </li>
      ))}
    </ul>
  );
}
