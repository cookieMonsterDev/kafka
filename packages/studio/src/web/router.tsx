import { createRouter, type RouterHistory } from '@tanstack/react-router';
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
import { streamsGroupDetailRoute } from './routes/streams-group-detail';
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
  streamsGroupDetailRoute,
  settingsRoute,
]);

/** Omitting `history` uses the browser URL. The docs demo passes a memory history so it never touches its iframe's URL. */
export function createStudioRouter(history?: RouterHistory) {
  return createRouter({ routeTree, defaultErrorComponent: PageError, history });
}

export type StudioRouter = ReturnType<typeof createStudioRouter>;

declare module '@tanstack/react-router' {
  interface Register {
    router: StudioRouter;
  }
}
