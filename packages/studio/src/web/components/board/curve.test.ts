import { describe, expect, it } from 'vitest';
import { edgeCurve, edgePathString, pointOnCurve } from './curve';

describe('pointOnCurve', () => {
  it('starts exactly at the first point and ends exactly at the second', () => {
    const curve = edgeCurve({ x: 0, y: 0 }, { x: 100, y: 40 });
    expect(pointOnCurve(curve, 0)).toEqual({ x: 0, y: 0 });
    expect(pointOnCurve(curve, 1)).toEqual({ x: 100, y: 40 });
  });

  it('sampling the reverse-direction curve traces the same line backward', () => {
    const forward = edgeCurve({ x: 0, y: 0 }, { x: 100, y: 40 });
    const backward = edgeCurve({ x: 100, y: 40 }, { x: 0, y: 0 });
    const t = 0.3;
    const forwardPoint = pointOnCurve(forward, t);
    const backwardPoint = pointOnCurve(backward, 1 - t);
    expect(backwardPoint.x).toBeCloseTo(forwardPoint.x);
    expect(backwardPoint.y).toBeCloseTo(forwardPoint.y);
  });
});

describe('edgePathString', () => {
  it('renders a cubic Bézier path with control points at the midpoint x', () => {
    const curve = edgeCurve({ x: 0, y: 0 }, { x: 100, y: 40 });
    expect(edgePathString(curve)).toBe('M 0 0 C 50 0, 50 40, 100 40');
  });
});
