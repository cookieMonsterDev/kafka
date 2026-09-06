import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const clusterRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
}).lazy(() => import('./cluster.lazy').then((module) => module.Route));
