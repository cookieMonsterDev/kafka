import type * as React from 'react';
import type { StudioEvent } from '../../../shared/contracts/event';
import type { RingBuffer } from '../../lib/ring-buffer';
import { edgeCurve, pointOnCurve } from './curve';
import type { BoardLayout, BoardNode } from './layout';
import { pathForTopic } from './layout';
import { toScreenPoint, type Viewport } from './viewport';

const PARTICLE_DURATION_MS = 900;
/** Read from CSS at attach time so the accent colour always matches the current theme token — already a full `oklch(...)` value, not bare components. */
const PARTICLE_COLOR_VAR = '--color-primary';
const FALLBACK_COLOR = 'oklch(0.72 0.16 163)';

interface Particle {
  readonly path: readonly string[];
  readonly startedAtVirtualMs: number;
}

export interface ParticleLayerHandle {
  setPaused(paused: boolean): void;
  setSpeed(multiplier: number): void;
  destroy(): void;
}

function resolveColor(canvas: HTMLCanvasElement): string {
  const value = getComputedStyle(canvas).getPropertyValue(PARTICLE_COLOR_VAR).trim();
  return value === '' ? FALLBACK_COLOR : value;
}

function nodeById(nodes: readonly BoardNode[], id: string): BoardNode | undefined {
  return nodes.find((node) => node.id === id);
}

function positionAlongPath(
  nodes: readonly BoardNode[],
  path: readonly string[],
  t: number,
  cssWidth: number,
  cssHeight: number,
  viewport: Viewport,
): { readonly x: number; readonly y: number } | null {
  if (path.length < 2) return null;
  const segments = path.length - 1;
  const clamped = Math.min(Math.max(t, 0), 1);
  const segmentPosition = clamped * segments;
  const segmentIndex = Math.min(Math.floor(segmentPosition), segments - 1);
  const localT = segmentPosition - segmentIndex;

  const from = nodeById(nodes, path[segmentIndex] ?? '');
  const to = nodeById(nodes, path[segmentIndex + 1] ?? '');
  if (from === undefined || to === undefined) return null;

  // Sampled from the exact same curve shape `topology.tsx` draws the edge with (control points at
  // the midpoint x) — a straight-line lerp between the two node centers would visibly cut the
  // corner the SVG path actually curves through. `edgeCurve(from, to)` always traces from `from`
  // (t=0) to `to` (t=1), so this is correct whether the particle is moving hub-outward or the
  // reverse — a reversed path just samples the mirror-image curve, which is the same line.
  const virtual = pointOnCurve(edgeCurve(from, to), localT);
  return toScreenPoint(virtual, cssWidth, cssHeight, viewport);
}

export interface ParticleLayerOptions {
  readonly layout: BoardLayout;
  readonly viewportRef: React.RefObject<Viewport>;
  readonly events: RingBuffer<StudioEvent>;
  /** `prefers-reduced-motion`: the loop never starts at all — paused, not merely slowed, and nothing repaints on an interval. */
  readonly reducedMotion: boolean;
}

/**
 * Draws message-flow particles on `canvas` from the board's real activity firehose — every
 * particle traces an event that actually happened (a produce or a tail delivery), spawned the
 * moment it appears in `events`. Runs its own `requestAnimationFrame` loop entirely outside React;
 * `viewportRef` is read fresh every frame so panning or zooming the SVG skeleton never leaves the
 * particles behind.
 */
export function attachParticleLayer(canvas: HTMLCanvasElement, options: ParticleLayerOptions): ParticleLayerHandle {
  const ctx = canvas.getContext('2d');
  let particles: Particle[] = [];
  let lastSeenEventId = 0;
  let paused = options.reducedMotion;
  let speed = 1;
  let virtualNow = 0;
  let lastRealTime = performance.now();
  let cssWidth = canvas.clientWidth;
  let cssHeight = canvas.clientHeight;
  let color = '#00c387';
  let frame = 0;
  let destroyed = false;

  function resize(): void {
    const dpr = window.devicePixelRatio || 1;
    cssWidth = canvas.clientWidth;
    cssHeight = canvas.clientHeight;
    canvas.width = Math.max(1, Math.round(cssWidth * dpr));
    canvas.height = Math.max(1, Math.round(cssHeight * dpr));
    ctx?.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  const resizeObserver = new ResizeObserver(resize);
  resizeObserver.observe(canvas);
  resize();
  color = resolveColor(canvas);

  function spawnFromEvent(event: StudioEvent): void {
    const path = pathForTopic(options.layout, event.topic);
    particles.push({
      path: event.kind === 'consume' ? [...path].reverse() : path,
      startedAtVirtualMs: virtualNow,
    });
  }

  function tick(): void {
    // A paused loop stops scheduling itself entirely rather than looping empty — nothing on this
    // canvas repaints, or even wakes up, until `setPaused(false)` kicks it off again.
    if (destroyed || paused || ctx === null) return;
    frame = requestAnimationFrame(tick);

    const realNow = performance.now();
    const deltaMs = realNow - lastRealTime;
    lastRealTime = realNow;
    virtualNow += deltaMs * speed;

    const recent = options.events.getSnapshot();
    for (const event of recent) {
      if (event.id > lastSeenEventId) spawnFromEvent(event);
    }
    if (recent.length > 0) lastSeenEventId = recent[recent.length - 1]?.id ?? lastSeenEventId;

    particles = particles.filter((particle) => virtualNow - particle.startedAtVirtualMs < PARTICLE_DURATION_MS);

    ctx.clearRect(0, 0, cssWidth, cssHeight);
    ctx.fillStyle = color;
    const viewport = options.viewportRef.current;
    for (const particle of particles) {
      const t = (virtualNow - particle.startedAtVirtualMs) / PARTICLE_DURATION_MS;
      const point = positionAlongPath(options.layout.nodes, particle.path, t, cssWidth, cssHeight, viewport);
      if (point === null) continue;

      // Fades in and out at the ends of its travel instead of popping in/out at full opacity.
      const fade = t < 0.15 ? t / 0.15 : t > 0.85 ? (1 - t) / 0.15 : 1;
      ctx.globalAlpha = Math.max(0, Math.min(1, fade));
      ctx.beginPath();
      ctx.arc(point.x, point.y, 3.5, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.globalAlpha = 1;
  }

  if (!paused) frame = requestAnimationFrame(tick);
  else if (!destroyed) resize(); // still-paused canvas at least reflects the current size once

  return {
    setPaused(next) {
      const wasPaused = paused;
      paused = next;
      if (wasPaused && !next) {
        lastRealTime = performance.now();
        frame = requestAnimationFrame(tick);
      }
    },
    setSpeed(multiplier) {
      speed = multiplier;
    },
    destroy() {
      destroyed = true;
      cancelAnimationFrame(frame);
      resizeObserver.disconnect();
    },
  };
}
