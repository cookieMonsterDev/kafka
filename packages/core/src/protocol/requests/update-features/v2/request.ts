import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema, type UpdateFeaturesRequestV1Fields } from '../v1/request';

export type UpdateFeaturesRequestV2Fields = UpdateFeaturesRequestV1Fields;

/** UpdateFeatures v2 keeps the v1 request wire format. */
export const updateFeaturesRequestV2 = defineRequest({
  apiKey: API_KEYS.UpdateFeatures,
  apiVersion: 2,
  apiName: 'UpdateFeatures',
  schema: requestSchema,
});
