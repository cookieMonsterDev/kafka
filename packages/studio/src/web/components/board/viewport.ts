import { VIEWBOX_HEIGHT, VIEWBOX_WIDTH } from './layout';

export interface Viewport {
  /** Pan offset, in the SVG's own (pre-fit-scale) units — same units the `viewBox` itself is defined in. */
  readonly x: number;
  readonly y: number;
  readonly scale: number;
}

export const DEFAULT_VIEWPORT: Viewport = { x: 0, y: 0, scale: 1 };
export const MIN_SCALE = 0.5;
export const MAX_SCALE = 2.5;

export function clampScale(scale: number): number {
  return Math.min(MAX_SCALE, Math.max(MIN_SCALE, scale));
}

/** The `<g>` the SVG skeleton wraps its nodes in — `translate` before `scale` so panning stays a constant screen distance regardless of zoom level. */
export function svgGroupTransform(viewport: Viewport): string {
  return `translate(${String(viewport.x)} ${String(viewport.y)}) scale(${String(viewport.scale)})`;
}

export interface Point {
  readonly x: number;
  readonly y: number;
}

/**
 * Maps a point in board-virtual space to CSS pixels within an element of the given size — the same
 * "contain" fit `viewBox preserveAspectRatio="xMidYMid meet"` performs, with the pan/zoom transform
 * layered underneath it. The canvas particle layer calls this every frame instead of measuring node
 * `DOMRect`s, so it can never drift from what the SVG is actually showing after a resize or zoom.
 */
export function toScreenPoint(point: Point, cssWidth: number, cssHeight: number, viewport: Viewport): Point {
  const fitScale = Math.min(cssWidth / VIEWBOX_WIDTH, cssHeight / VIEWBOX_HEIGHT);
  const fitOffsetX = (cssWidth - VIEWBOX_WIDTH * fitScale) / 2;
  const fitOffsetY = (cssHeight - VIEWBOX_HEIGHT * fitScale) / 2;

  const outerX = point.x * viewport.scale + viewport.x;
  const outerY = point.y * viewport.scale + viewport.y;

  return { x: fitOffsetX + outerX * fitScale, y: fitOffsetY + outerY * fitScale };
}
