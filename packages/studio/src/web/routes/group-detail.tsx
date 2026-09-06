import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const groupDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/groups/$groupId',
}).lazy(() => import('./group-detail.lazy').then((module) => module.Route));
