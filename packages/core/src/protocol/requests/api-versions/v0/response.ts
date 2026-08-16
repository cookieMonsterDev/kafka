import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, object } from '../../../schema';

/**
 * ApiVersionResponse => ErrorCode [ApiVersions]
 *   ErrorCode => INT16
 *   ApiVersions => ApiKey MinVersion MaxVersion
 *     ApiKey => INT16
 *     MinVersion => INT16
 *     MaxVersion => INT16
 */
const apiVersionEntrySchema = object([field('apiKey', int16), field('minVersion', int16), field('maxVersion', int16)]);

const bodySchema = object([field('errorCode', int16), field('apiVersions', array(apiVersionEntrySchema))]);

export const apiVersionsResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
