import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

export const consoleRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/console',
}).lazy(() => import('./console.lazy').then((module) => module.Route));
