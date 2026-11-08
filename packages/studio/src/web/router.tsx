import { createRouter } from '@tanstack/react-router';
import { debugRoute } from './routes/debug';
import { boardRoute } from './routes/board';
import { clusterRoute } from './routes/cluster';
import { consoleRoute } from './routes/console';
import { groupDetailRoute } from './routes/group-detail';
import { groupsRoute } from './routes/groups';
import { messagesRoute } from './routes/messages';
import { producerRoute } from './routes/producer';
import { rootRoute, PageError } from './routes/root';
import { settingsRoute } from './routes/settings';
import { shareGroupDetailRoute } from './routes/share-group-detail';
import { topicDetailRoute } from './routes/topic-detail';
import { topicsRoute } from './routes/topics';

const routeTree = rootRoute.addChildren([
  debugRoute,
  clusterRoute,
  consoleRoute,
  topicsRoute,
  topicDetailRoute,
  producerRoute,
  messagesRoute,
  boardRoute,
  groupsRoute,
  groupDetailRoute,
  shareGroupDetailRoute,
  settingsRoute,
]);

export const router = createRouter({ routeTree, defaultErrorComponent: PageError });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
