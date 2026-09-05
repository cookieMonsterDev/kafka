import { createRouter } from '@tanstack/react-router';
import { clusterRoute } from './routes/cluster';
import { producerRoute } from './routes/producer';
import { rootRoute } from './routes/root';
import { topicDetailRoute } from './routes/topic-detail';
import { topicsRoute } from './routes/topics';

const routeTree = rootRoute.addChildren([clusterRoute, topicsRoute, topicDetailRoute, producerRoute]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
