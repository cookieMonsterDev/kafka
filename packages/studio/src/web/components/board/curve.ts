import type { Point } from './viewport';

export interface CubicEdge {
  readonly p0: Point;
  readonly p1: Point;
  readonly p2: Point;
  readonly p3: Point;
}

/**
 * The one curve shape every board edge uses: a horizontal S-curve between two points, control
 * points pulled to the midpoint x. Both the SVG skeleton (as a `path` string) and the canvas
 * particle layer (sampled point-by-point) build their curve from this same function — a particle
 * computed from anything else would visibly cut the corner the SVG line actually curves through.
 */
export function edgeCurve(from: Point, to: Point): CubicEdge {
  const controlX = (from.x + to.x) / 2;
  return {
    p0: from,
    p1: { x: controlX, y: from.y },
    p2: { x: controlX, y: to.y },
    p3: to,
  };
}

export function edgePathString(curve: CubicEdge): string {
  const { p0, p1, p2, p3 } = curve;
  return `M ${String(p0.x)} ${String(p0.y)} C ${String(p1.x)} ${String(p1.y)}, ${String(p2.x)} ${String(p2.y)}, ${String(p3.x)} ${String(p3.y)}`;
}

/** A point on the curve at `t` (0 = start, 1 = end) — the standard cubic Bézier formula. */
export function pointOnCurve(curve: CubicEdge, t: number): Point {
  const { p0, p1, p2, p3 } = curve;
  const mt = 1 - t;
  const a = mt * mt * mt;
  const b = 3 * mt * mt * t;
  const c = 3 * mt * t * t;
  const d = t * t * t;
  return {
    x: a * p0.x + b * p1.x + c * p2.x + d * p3.x,
    y: a * p0.y + b * p1.y + c * p2.y + d * p3.y,
  };
}
