import { useEffect, useMemo, useRef, useState } from 'react';
import { createRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import type { StudioEvent } from '../../shared/contracts/event';
import { ActionsDock } from '../components/board/actions-dock';
import { BoardControls, usePrefersReducedMotion } from '../components/board/controls';
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

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null) return;
    const handle = attachParticleLayer(canvas, { layout, viewportRef, events: eventsBuffer, reducedMotion });
    particleLayerRef.current = handle;
    return () => {
      handle.destroy();
      particleLayerRef.current = null;
    };
    // Re-attached only when the topology itself changes shape — pause/speed are pushed imperatively below.
  }, [layout, eventsBuffer]);

  useEffect(() => {
    particleLayerRef.current?.setPaused(paused);
  }, [paused]);

  // Reduced motion turning on mid-session pauses the board; turning it back off never forces a
  // resume the viewer didn't ask for — a `paused` state that started `true` covers that already.
  useEffect(() => {
    if (reducedMotion) setPaused(true);
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
      onTogglePaused={() => setPaused((current) => !current)}
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
