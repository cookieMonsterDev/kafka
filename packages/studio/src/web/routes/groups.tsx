import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const groupsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/groups',
}).lazy(() => import('./groups.lazy').then((module) => module.Route));
