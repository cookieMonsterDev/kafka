import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export interface MessagesSearch {
  /** Prefills the topic picker — the board's "Tail this topic" action links in with this. */
  readonly topic?: string;
}

export const messagesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/messages',
  validateSearch: (search: Record<string, unknown>): MessagesSearch => ({
    topic: typeof search.topic === 'string' ? search.topic : undefined,
  }),
}).lazy(() => import('./messages.lazy').then((module) => module.Route));
