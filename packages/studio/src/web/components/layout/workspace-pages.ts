import { Layers, MessageSquare, Send, Server, Settings, Users, Workflow, type LucideIcon } from 'lucide-react';
import type { RegisteredRouter, RootRouteId, RouteIds } from '@tanstack/react-router';

/** Derived from the router's own registered route tree rather than hand-listed, so a renamed or removed route fails typecheck here instead of silently drifting. `RouteIds` also includes the root itself, which is never a navigation target. */
export type PageRoute = Exclude<RouteIds<RegisteredRouter['routeTree']>, RootRouteId>;

export interface WorkspacePage {
  readonly label: string;
  readonly icon: LucideIcon;
  readonly to: PageRoute;
}

/** The seven top-level workspace areas — one list shared by the sidebar nav and the command palette, so a new route is never added to one and forgotten in the other. */
export const WORKSPACE_PAGES: readonly WorkspacePage[] = [
  { label: 'Cluster', icon: Server, to: '/' },
  { label: 'Topics', icon: Layers, to: '/topics' },
  { label: 'Producer', icon: Send, to: '/producer' },
  { label: 'Messages', icon: MessageSquare, to: '/messages' },
  { label: 'Board', icon: Workflow, to: '/board' },
  { label: 'Consumer groups', icon: Users, to: '/groups' },
  { label: 'Settings', icon: Settings, to: '/settings' },
];
