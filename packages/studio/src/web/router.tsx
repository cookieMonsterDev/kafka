import { createRouter } from '@tanstack/react-router';
import { boardRoute } from './routes/board';
import { clusterRoute } from './routes/cluster';
import { groupDetailRoute } from './routes/group-detail';
import { groupsRoute } from './routes/groups';
import { messagesRoute } from './routes/messages';
import { producerRoute } from './routes/producer';
import { rootRoute } from './routes/root';
import { shareGroupDetailRoute } from './routes/share-group-detail';
import { topicDetailRoute } from './routes/topic-detail';
import { topicsRoute } from './routes/topics';

const routeTree = rootRoute.addChildren([
  clusterRoute,
  topicsRoute,
  topicDetailRoute,
  producerRoute,
  messagesRoute,
  boardRoute,
  groupsRoute,
  groupDetailRoute,
  shareGroupDetailRoute,
]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
