import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export interface ProducerSearch {
  /** Prefills the topic picker — the board's "Produce here" action links in with this. */
  readonly topic?: string;
}

export const producerRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/producer',
  validateSearch: (search: Record<string, unknown>): ProducerSearch => ({
    topic: typeof search.topic === 'string' ? search.topic : undefined,
  }),
}).lazy(() => import('./producer.lazy').then((module) => module.Route));
