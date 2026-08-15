import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const groupProtocolSchema = object([field('name', string), field('metadata', bytes)]);
const requestSchema = object([
  field('groupId', string),
  field('sessionTimeout', int32),
  field('rebalanceTimeout', int32),
  field('memberId', string),
  field('protocolType', string),
  field('groupProtocols', array(groupProtocolSchema)),
]);

export const joinGroupRequestV2 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 2,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
