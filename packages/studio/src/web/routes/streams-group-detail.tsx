import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const streamsGroupDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/streams-groups/$groupId',
}).lazy(() => import('./streams-group-detail.lazy').then((module) => module.Route));
