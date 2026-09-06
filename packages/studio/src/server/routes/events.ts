import type { StudioEventBus } from '../kafka/events';
import type { Router } from '../router';
import { openSseStream } from '../sse';

export interface EventsRouteContext {
  readonly events: StudioEventBus;
}

/** `GET /api/events` — the board's live activity firehose. Streams every future produce/tail event; carries no backlog. */
export function registerEventRoutes(router: Router, context: EventsRouteContext): void {
  router.get('/api/events', (req, res) => {
    const stream = openSseStream(req, res, () => unsubscribe());
    const unsubscribe = context.events.subscribe((event) => stream.send('activity', event));
  });
}
