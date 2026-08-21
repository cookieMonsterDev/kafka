import {
  boolean,
  compactArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface FeatureUpdateV0 {
  feature: string;
  maxVersionLevel: number;
  allowDowngrade: boolean;
}

export interface UpdateFeaturesRequestV0Fields {
  timeoutMs: number;
  featureUpdates: FeatureUpdateV0[];
}

/**
 * UpdateFeatures Request (Version: 0) => timeout_ms [feature_updates] TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const featureUpdateSchema = flexibleObject([
  field('feature', compactString),
  field('maxVersionLevel', int16),
  field('allowDowngrade', boolean),
]);
export const requestSchema = flexibleObject([
  field('timeoutMs', int32),
  field('featureUpdates', compactArray(featureUpdateSchema)),
]);

export const updateFeaturesRequestV0 = defineRequest({
  apiKey: API_KEYS.UpdateFeatures,
  apiVersion: 0,
  apiName: 'UpdateFeatures',
  schema: requestSchema,
});
