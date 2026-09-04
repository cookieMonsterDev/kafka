import { createRoute, createRouter } from '@tanstack/react-router';
import { rootRoute } from './routes/root';

const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: () => null });

const routeTree = rootRoute.addChildren([indexRoute]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
