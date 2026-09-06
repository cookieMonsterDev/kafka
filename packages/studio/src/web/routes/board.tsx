import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const boardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/board',
}).lazy(() => import('./board.lazy').then((module) => module.Route));
