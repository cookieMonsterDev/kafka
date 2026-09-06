import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const topicsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/topics',
}).lazy(() => import('./topics.lazy').then((module) => module.Route));
