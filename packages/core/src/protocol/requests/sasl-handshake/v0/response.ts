import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, object, string } from '../../../schema.js';

/**
 * SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms]
 *   error_code => INT16
 *   enabled_mechanisms => STRING
 */
const bodySchema = object([field('errorCode', int16), field('enabledMechanisms', array(string))]);

export const saslHandshakeResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
