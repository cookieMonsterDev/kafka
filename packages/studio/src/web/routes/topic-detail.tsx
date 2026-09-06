import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const topicDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/topics/$name',
}).lazy(() => import('./topic-detail.lazy').then((module) => module.Route));
