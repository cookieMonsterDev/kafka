import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const shareGroupDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/share-groups/$groupId',
}).lazy(() => import('./share-group-detail.lazy').then((module) => module.Route));
