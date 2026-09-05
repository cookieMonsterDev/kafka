import { z } from 'zod';

/**
 * `produce` and `consume` are activity this studio process itself generated (the producer
 * playground, a burst job, a live message tail) — the only Kafka traffic a local tool can observe
 * without a cluster-wide tracing system. The board's particle layer is real telemetry of that
 * traffic, not a simulation.
 */
export const studioEventKindSchema = z.enum(['produce', 'consume']);
export type StudioEventKind = z.infer<typeof studioEventKindSchema>;

export interface StudioEvent {
  readonly id: number;
  readonly kind: StudioEventKind;
  readonly topic: string;
  /** `null` when the activity spans more than one partition (e.g. a tail with no partition filter). */
  readonly partition: number | null;
  /** How many records this one frame represents — a burst reports progress in batches, not per record. */
  readonly count: number;
  readonly bytes: number;
  readonly timestamp: number;
}

export const studioEventSchema = z.object({
  id: z.number().int().nonnegative(),
  kind: studioEventKindSchema,
  topic: z.string().min(1),
  partition: z.number().int().nonnegative().nullable(),
  count: z.number().int().positive(),
  bytes: z.number().int().nonnegative(),
  timestamp: z.number().int().nonnegative(),
}) satisfies z.ZodType<StudioEvent>;
