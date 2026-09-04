import { createRouter } from '@tanstack/react-router';
import { clusterRoute } from './routes/cluster';
import { rootRoute } from './routes/root';
import { topicDetailRoute } from './routes/topic-detail';
import { topicsRoute } from './routes/topics';

const routeTree = rootRoute.addChildren([clusterRoute, topicsRoute, topicDetailRoute]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
