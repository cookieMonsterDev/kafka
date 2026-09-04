import { createRouter } from '@tanstack/react-router';
import { clusterRoute } from './routes/cluster';
import { rootRoute } from './routes/root';

const routeTree = rootRoute.addChildren([clusterRoute]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
