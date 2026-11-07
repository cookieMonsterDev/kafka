import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';
export const debugRoute = createRoute({ getParentRoute: () => rootRoute, path: '/debug' }).lazy(() =>
  import('./debug.lazy').then((module) => module.Route),
);
