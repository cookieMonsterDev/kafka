import { useEffect, useMemo, useRef, useState } from 'react';
import { createRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import type { StudioEvent } from '../../shared/contracts/event';
import { ActionsDock } from '../components/board/actions-dock';
import { BoardControls } from '../components/board/controls';
import type { BoardNode } from '../components/board/layout';
import { layoutBoard } from '../components/board/layout';
import { BoardMetrics } from '../components/board/metrics';
import { Topology } from '../components/board/topology';
import { attachParticleLayer, type ParticleLayerHandle } from '../components/board/particles';
import { DEFAULT_VIEWPORT, type Viewport } from '../components/board/viewport';
import { PageLayout } from '../components/layout/page';
import { groupQueryKeys, listGroups } from '../lib/groups-api';
import { RingBuffer, useRingBuffer } from '../lib/ring-buffer';
import { useActivityFeed } from '../lib/sse';
import { listTopics, topicQueryKeys } from '../lib/topics-api';
import { usePrefersReducedMotion } from '../lib/use-prefers-reduced-motion';
import { rootRoute } from './root';

export const boardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/board',
  component: BoardPage,
});

const EVENTS_BUFFER_CAPACITY = 300;

function BoardPage() {
  const topicsQuery = useQuery({ queryKey: topicQueryKeys.list(), queryFn: listTopics });
  const groupsQuery = useQuery({ queryKey: groupQueryKeys.list(), queryFn: listGroups });

  const layout = useMemo(
    () => layoutBoard(topicsQuery.data?.topics ?? [], groupsQuery.data?.groups ?? []),
    [topicsQuery.data, groupsQuery.data],
  );

  const eventsBuffer = useMemo(() => new RingBuffer<StudioEvent>(EVENTS_BUFFER_CAPACITY), []);
  const events = useRingBuffer(eventsBuffer);
  const { connected } = useActivityFeed(eventsBuffer);

  const viewportRef = useRef<Viewport>(DEFAULT_VIEWPORT);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const particleLayerRef = useRef<ParticleLayerHandle | null>(null);

  const reducedMotion = usePrefersReducedMotion();
  const [paused, setPaused] = useState(reducedMotion);
  const [speed, setSpeed] = useState('1');

  const [selectedNode, setSelectedNode] = useState<BoardNode | null>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);

  /** The one place `paused` is pushed into the imperative particle layer — every source of a
   * pause/resume (the toolbar button, reduced motion turning on) calls this directly instead of
   * only setting React state and relying on an effect to notice, so there's no second effect
   * reacting to the first. */
  function setPausedAndSync(next: boolean): void {
    setPaused(next);
    particleLayerRef.current?.setPaused(next);
  }

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null) return;
    const handle = attachParticleLayer(canvas, { layout, viewportRef, events: eventsBuffer, reducedMotion });
    // Re-applies the current `paused` state right after attaching, rather than depending on it:
    // `attachParticleLayer` only seeds its *initial* paused flag from `reducedMotion` (see
    // particles.ts), which can be stale by the time a later layout change re-attaches this layer
    // (the viewer may have pressed play since). `setPausedAndSync` is the live source of truth for
    // every other change; this is just catching the freshly attached layer up to it.
    handle.setPaused(paused);
    particleLayerRef.current = handle;
    return () => {
      handle.destroy();
      particleLayerRef.current = null;
    };
    // Re-attached only when the topology itself changes shape — pushing a new `paused` value
    // through `setPausedAndSync` deliberately does not re-run this effect.
  }, [layout, eventsBuffer]);

  // Reduced motion turning on mid-session pauses the board; turning it back off never forces a
  // resume the viewer didn't ask for — a `paused` state that started `true` covers that already.
  useEffect(() => {
    if (reducedMotion) setPausedAndSync(true);
  }, [reducedMotion]);

  useEffect(() => {
    particleLayerRef.current?.setSpeed(Number(speed));
  }, [speed]);

  function handleNodeSelect(node: BoardNode, rect: DOMRect): void {
    setSelectedNode(node);
    setAnchorRect(rect);
  }

  const toolbar = (
    <BoardControls
      paused={paused}
      onTogglePaused={() => setPausedAndSync(!paused)}
      speed={speed}
      onSpeedChange={setSpeed}
      reducedMotion={reducedMotion}
    />
  );

  const rail = (
    <BoardMetrics
      events={events}
      topicCount={topicsQuery.data?.topics.length ?? 0}
      groupCount={groupsQuery.data?.groups.length ?? 0}
      live={connected}
    />
  );

  return (
    <PageLayout toolbar={toolbar} rail={rail} railLabel="Cluster activity">
      <section aria-label="Flow board" className="relative h-144 max-h-[70vh] w-full min-w-0">
        <Topology
          layout={layout}
          viewportRef={viewportRef}
          activeNodeId={selectedNode?.id ?? null}
          onNodeSelect={handleNodeSelect}
        />
        <canvas ref={canvasRef} aria-hidden="true" className="pointer-events-none absolute inset-0 h-full w-full" />
        <ActionsDock
          node={selectedNode}
          anchorRect={anchorRect}
          onClose={() => {
            setSelectedNode(null);
            setAnchorRect(null);
          }}
        />
      </section>
    </PageLayout>
  );
}
