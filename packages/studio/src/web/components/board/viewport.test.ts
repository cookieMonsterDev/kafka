import { describe, expect, it } from 'vitest';
import { VIEWBOX_HEIGHT, VIEWBOX_WIDTH } from './layout';
import { clampScale, DEFAULT_VIEWPORT, MAX_SCALE, MIN_SCALE, svgGroupTransform, toScreenPoint } from './viewport';

describe('clampScale', () => {
  it('leaves an in-range scale untouched', () => {
    expect(clampScale(1)).toBe(1);
  });

  it('clamps to the minimum and maximum', () => {
    expect(clampScale(0)).toBe(MIN_SCALE);
    expect(clampScale(10)).toBe(MAX_SCALE);
  });
});

describe('svgGroupTransform', () => {
  it('renders a translate-then-scale transform string', () => {
    expect(svgGroupTransform({ x: 10, y: -5, scale: 1.5 })).toBe('translate(10 -5) scale(1.5)');
  });
});

describe('toScreenPoint', () => {
  it('centers the origin when the element matches the viewBox aspect ratio at the default viewport', () => {
    const point = toScreenPoint({ x: 0, y: 0 }, VIEWBOX_WIDTH, VIEWBOX_HEIGHT, DEFAULT_VIEWPORT);
    expect(point).toEqual({ x: 0, y: 0 });
  });

  it('maps the viewBox center to the element center', () => {
    const point = toScreenPoint(
      { x: VIEWBOX_WIDTH / 2, y: VIEWBOX_HEIGHT / 2 },
      VIEWBOX_WIDTH,
      VIEWBOX_HEIGHT,
      DEFAULT_VIEWPORT,
    );
    expect(point).toEqual({ x: VIEWBOX_WIDTH / 2, y: VIEWBOX_HEIGHT / 2 });
  });

  it('fits the whole viewBox into a proportionally smaller element', () => {
    const point = toScreenPoint({ x: VIEWBOX_WIDTH, y: 0 }, VIEWBOX_WIDTH / 2, VIEWBOX_HEIGHT / 2, DEFAULT_VIEWPORT);
    // fitScale = min(0.5, 0.5) = 0.5, so the viewBox's far edge lands exactly on the element's far edge.
    expect(point.x).toBeCloseTo(VIEWBOX_WIDTH / 2);
  });

  it('applies pan before the fit scale', () => {
    const point = toScreenPoint({ x: 0, y: 0 }, VIEWBOX_WIDTH, VIEWBOX_HEIGHT, { x: 100, y: 0, scale: 1 });
    expect(point.x).toBeCloseTo(100);
  });

  it('applies zoom around the viewBox origin', () => {
    const point = toScreenPoint({ x: 10, y: 0 }, VIEWBOX_WIDTH, VIEWBOX_HEIGHT, { x: 0, y: 0, scale: 2 });
    expect(point.x).toBeCloseTo(20);
  });
});
