import type { ResponseDefinition } from '../../../schema';
import {
  updateFeaturesResponseV0,
  type UpdateFeaturesResponseV0Body,
  type UpdatableFeatureResult,
} from '../v0/response';

export type UpdateFeaturesResponseV1Body = UpdateFeaturesResponseV0Body;

export type { UpdatableFeatureResult };

/** UpdateFeatures v1 has the same response wire format and error semantics as v0. */
export const updateFeaturesResponseV1: ResponseDefinition<UpdateFeaturesResponseV1Body> = updateFeaturesResponseV0;
