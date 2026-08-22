import {
  boolean,
  compactArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface FeatureUpdateV1 {
  feature: string;
  maxVersionLevel: number;
  upgradeType: number;
}

export interface UpdateFeaturesRequestV1Fields {
  timeoutMs: number;
  featureUpdates: FeatureUpdateV1[];
  validateOnly: boolean;
}

/**
 * UpdateFeatures Request (Version: 1) => timeout_ms [feature_updates] validate_only TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const featureUpdateSchema = flexibleObject([
  field('feature', compactString),
  field('maxVersionLevel', int16),
  field('upgradeType', int8),
]);
export const requestSchema = flexibleObject([
  field('timeoutMs', int32),
  field('featureUpdates', compactArray(featureUpdateSchema)),
  field('validateOnly', boolean),
]);

export const updateFeaturesRequestV1 = defineRequest({
  apiKey: API_KEYS.UpdateFeatures,
  apiVersion: 1,
  apiName: 'UpdateFeatures',
  schema: requestSchema,
});
