import { useCallback, useEffect, useRef } from 'react';
import type * as React from 'react';
import { Database, Hash, Minus, Plus, RotateCcw, Server, Users } from 'lucide-react';
import { Button } from '../ui/button';
import { cn } from '../../lib/utils';
import { topicAccentClass } from '../../lib/topic-accent';
import { edgeCurve, edgePathString } from './curve';
import type { BoardEdge, BoardLayout, BoardNode } from './layout';
import { CARD_HEIGHT, CARD_WIDTH, VIEWBOX_HEIGHT, VIEWBOX_WIDTH } from './layout';
import { clampScale, DEFAULT_VIEWPORT, svgGroupTransform, type Viewport } from './viewport';

const ZOOM_STEP = 1.2;

function edgePath(nodes: readonly BoardNode[], edge: BoardEdge): string {
  const from = nodes.find((node) => node.id === edge.from);
  const to = nodes.find((node) => node.id === edge.to);
  if (from === undefined || to === undefined) return '';
  return edgePathString(edgeCurve(from, to));
}

const NODE_LABEL: Record<BoardNode['kind'], string> = {
  studio: 'This studio',
  cluster: 'Cluster',
  topic: 'Topic',
  group: 'Consumer group',
};

const NODE_ICON: Record<BoardNode['kind'], React.ComponentType<{ className?: string }>> = {
  studio: Server,
  cluster: Database,
  topic: Hash,
  group: Users,
};

/**
 * A glowing outline square, not a solid fill — a wireframe icon on a tinted, translucent
 * background reads as "live signal" the way the studio's own brand mark does. Written as fully
 * literal class strings (never `` `bg-${token}` `` template interpolation) because Tailwind finds
 * utility classes by scanning source text, not by evaluating it — an interpolated class name never
 * makes it into the generated CSS.
 */
const CHART_ICON_CLASSES: Record<string, string> = {
  'chart-1': 'bg-chart-1/10 text-chart-1 ring-1 ring-chart-1/40',
  'chart-2': 'bg-chart-2/10 text-chart-2 ring-1 ring-chart-2/40',
  'chart-3': 'bg-chart-3/10 text-chart-3 ring-1 ring-chart-3/40',
  'chart-4': 'bg-chart-4/10 text-chart-4 ring-1 ring-chart-4/40',
  'chart-5': 'bg-chart-5/10 text-chart-5 ring-1 ring-chart-5/40',
};
const PRIMARY_ICON_CLASSES = 'bg-primary/10 text-primary ring-1 ring-primary/40';
const NEUTRAL_ICON_CLASSES = 'bg-muted-foreground/10 text-muted-foreground ring-1 ring-muted-foreground/30';

const CHART_BORDER_CLASSES: Record<string, string> = {
  'chart-1': 'border-chart-1/40',
  'chart-2': 'border-chart-2/40',
  'chart-3': 'border-chart-3/40',
  'chart-4': 'border-chart-4/40',
  'chart-5': 'border-chart-5/40',
};
const PRIMARY_BORDER_CLASSES = 'border-primary/40';
const NEUTRAL_BORDER_CLASSES = 'border-muted-foreground/30';

/** A topic keeps the same categorical accent it has everywhere else in the studio (`topicAccentClass` returns `bg-chart-N`). */
function iconClassName(node: BoardNode): string {
  if (node.kind === 'topic') {
    const token = topicAccentClass(node.label).replace('bg-', '');
    return CHART_ICON_CLASSES[token] ?? NEUTRAL_ICON_CLASSES;
  }
  if (node.kind === 'cluster' || node.kind === 'studio') return PRIMARY_ICON_CLASSES;
  return NEUTRAL_ICON_CLASSES;
}

/** The card's own border always echoes its icon's accent — not just on hover/active — so the tint reads as this node's identity, not a transient state. */
function borderClassName(node: BoardNode): string {
  if (node.kind === 'topic') {
    const token = topicAccentClass(node.label).replace('bg-', '');
    return CHART_BORDER_CLASSES[token] ?? NEUTRAL_BORDER_CLASSES;
  }
  if (node.kind === 'cluster' || node.kind === 'studio') return PRIMARY_BORDER_CLASSES;
  return NEUTRAL_BORDER_CLASSES;
}

interface NodeCardProps {
  readonly node: BoardNode;
  readonly active: boolean;
  readonly onSelect: (node: BoardNode, rect: DOMRect) => void;
}

/**
 * A rounded card rendered through `foreignObject` — icon square, title, and a real fact as the
 * subtitle (partition count, protocol), the same shape the rest of the studio already uses for a
 * "what is this" summary. `foreignObject` content scales and pans with the surrounding `<g>` like
 * any other SVG child, so this costs nothing over hand-drawn shapes.
 */
function NodeCard({ node, active, onSelect }: NodeCardProps) {
  const interactive = node.kind === 'topic' || node.kind === 'group';
  const Icon = NODE_ICON[node.kind];

  const card = (
    <div
      className={cn(
        'flex h-full w-full items-center gap-2.5 rounded-xl border bg-card px-3 text-card-foreground shadow-sm transition-colors',
        active ? 'border-primary ring-2 ring-primary/40' : borderClassName(node),
        interactive && 'cursor-pointer hover:border-primary/50',
      )}
    >
      <span className={cn('flex size-9 shrink-0 items-center justify-center rounded-lg', iconClassName(node))}>
        <Icon className="size-4.5" aria-hidden="true" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block truncate text-sm font-semibold">{node.label}</span>
        {node.subtitle !== undefined && (
          <span className="block truncate text-xs text-muted-foreground">{node.subtitle}</span>
        )}
      </span>
    </div>
  );

  return (
    <g transform={`translate(${String(node.x - CARD_WIDTH / 2)} ${String(node.y - CARD_HEIGHT / 2)})`}>
      <foreignObject
        width={CARD_WIDTH}
        height={CARD_HEIGHT}
        {...(interactive
          ? {
              role: 'button',
              tabIndex: 0,
              'aria-label': `${NODE_LABEL[node.kind]} ${node.label}`,
              'aria-pressed': active,
              onClick: (event: React.MouseEvent<SVGForeignObjectElement>) =>
                onSelect(node, event.currentTarget.getBoundingClientRect()),
              onKeyDown: (event: React.KeyboardEvent<SVGForeignObjectElement>) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  onSelect(node, event.currentTarget.getBoundingClientRect());
                }
              },
            }
          : {})}
      >
        {card}
      </foreignObject>
    </g>
  );
}

export interface TopologyProps {
  readonly layout: BoardLayout;
  readonly viewportRef: React.RefObject<Viewport>;
  readonly activeNodeId: string | null;
  readonly onNodeSelect: (node: BoardNode, rect: DOMRect) => void;
}

/**
 * The board's static skeleton: an SVG rendered from {@link layoutBoard}'s fixed coordinates, with
 * pan (drag or single-finger touch) and zoom (wheel, pinch, or the on-screen buttons) applied to a
 * `<g>` element directly — not through React state — so dragging doesn't re-render the whole tree
 * every pointer move. `viewportRef` is the single source of truth the canvas particle layer
 * (`particles.ts`) also reads every animation frame, which is what keeps the two layers aligned.
 */
export function Topology({ layout, viewportRef, activeNodeId, onNodeSelect }: TopologyProps) {
  const groupRef = useRef<SVGGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const pointers = useRef(new Map<number, { x: number; y: number }>());
  const pinchStartDistance = useRef<number | null>(null);
  const pinchStartScale = useRef(1);

  const applyViewport = useCallback(
    (next: Viewport) => {
      viewportRef.current = next;
      groupRef.current?.setAttribute('transform', svgGroupTransform(next));
    },
    [viewportRef],
  );

  const zoomBy = useCallback(
    (factor: number) => {
      const current = viewportRef.current;
      applyViewport({ ...current, scale: clampScale(current.scale * factor) });
    },
    [applyViewport, viewportRef],
  );

  useEffect(() => {
    applyViewport(viewportRef.current);
    // Only on mount — the transform is otherwise only ever changed imperatively.
  }, []);

  function handlePointerDown(event: React.PointerEvent<SVGSVGElement>): void {
    if (event.target instanceof Element && event.target.closest('[role="button"]')) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    pointers.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    if (pointers.current.size === 2) {
      const [a, b] = [...pointers.current.values()];
      if (a && b) {
        pinchStartDistance.current = Math.hypot(a.x - b.x, a.y - b.y);
        pinchStartScale.current = viewportRef.current.scale;
      }
    }
  }

  function handlePointerMove(event: React.PointerEvent<SVGSVGElement>): void {
    const previous = pointers.current.get(event.pointerId);
    if (previous === undefined) return;
    const current = { x: event.clientX, y: event.clientY };
    pointers.current.set(event.pointerId, current);

    if (pointers.current.size === 2 && pinchStartDistance.current !== null) {
      const [a, b] = [...pointers.current.values()];
      if (a && b) {
        const distance = Math.hypot(a.x - b.x, a.y - b.y);
        const scale = clampScale(pinchStartScale.current * (distance / pinchStartDistance.current));
        applyViewport({ ...viewportRef.current, scale });
      }
      return;
    }

    if (pointers.current.size === 1) {
      const dx = current.x - previous.x;
      const dy = current.y - previous.y;
      const bounds = containerRef.current?.getBoundingClientRect();
      const fitScale = bounds ? Math.min(bounds.width / VIEWBOX_WIDTH, bounds.height / VIEWBOX_HEIGHT) : 1;
      const viewport = viewportRef.current;
      applyViewport({ ...viewport, x: viewport.x + dx / fitScale, y: viewport.y + dy / fitScale });
    }
  }

  function handlePointerUp(event: React.PointerEvent<SVGSVGElement>): void {
    pointers.current.delete(event.pointerId);
    if (pointers.current.size < 2) pinchStartDistance.current = null;
  }

  function handleWheel(event: React.WheelEvent<SVGSVGElement>): void {
    event.preventDefault();
    zoomBy(event.deltaY < 0 ? ZOOM_STEP : 1 / ZOOM_STEP);
  }

  return (
    <div
      ref={containerRef}
      className="relative h-full min-h-96 w-full overflow-hidden rounded-xl border border-border bg-card"
    >
      <svg
        viewBox={`0 0 ${String(VIEWBOX_WIDTH)} ${String(VIEWBOX_HEIGHT)}`}
        className="h-full w-full touch-none"
        role="group"
        aria-label="Cluster topology"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
        onWheel={handleWheel}
      >
        <g ref={groupRef} transform={svgGroupTransform(DEFAULT_VIEWPORT)}>
          <g className="stroke-border" fill="none" strokeWidth={1.5}>
            {layout.edges.map((edge) => (
              <path key={edge.id} d={edgePath(layout.nodes, edge)} />
            ))}
          </g>
          {layout.nodes.map((node) => (
            <NodeCard key={node.id} node={node} active={node.id === activeNodeId} onSelect={onNodeSelect} />
          ))}
        </g>
      </svg>

      <div className="absolute right-3 bottom-3 flex flex-col gap-1 rounded-lg border border-border bg-card/95 p-1 shadow-sm">
        <Button type="button" variant="ghost" size="icon-sm" aria-label="Zoom in" onClick={() => zoomBy(ZOOM_STEP)}>
          <Plus className="size-4" aria-hidden="true" />
        </Button>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          aria-label="Zoom out"
          onClick={() => zoomBy(1 / ZOOM_STEP)}
        >
          <Minus className="size-4" aria-hidden="true" />
        </Button>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          aria-label="Reset view"
          onClick={() => applyViewport(DEFAULT_VIEWPORT)}
        >
          <RotateCcw className="size-4" aria-hidden="true" />
        </Button>
      </div>
    </div>
  );
}
