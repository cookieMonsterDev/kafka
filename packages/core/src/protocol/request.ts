import { Encoder } from './encoder';
import { usesFlexibleRequestHeader } from './flexible';

export interface CreateRequestOptions {
  correlationId: number;
  clientId: string;
  request: {
    apiKey: number;
    apiVersion: number;
    encode(): Promise<Encoder>;
  };
}

/**
 * Wraps an already-encoded request body with the shared Kafka request header and the leading
 * length prefix every request/response on the wire carries.
 *
 * Header v1 (non-flexible): api key, api version, correlation id, client id.
 * Header v2 (flexible versions, including ApiVersions v3+): the same fields plus an empty
 * TAG_BUFFER after client id (KIP-482). ApiVersions *responses* still use header v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export async function createRequest({ correlationId, clientId, request }: CreateRequestOptions): Promise<Encoder> {
  const payload = await request.encode();
  const requestPayload = new Encoder()
    .writeInt16(request.apiKey)
    .writeInt16(request.apiVersion)
    .writeInt32(correlationId)
    .writeString(clientId);

  if (usesFlexibleRequestHeader(request.apiKey, request.apiVersion)) {
    requestPayload.writeUVarInt(0);
  }

  requestPayload.writeEncoder(payload);

  return new Encoder().writeInt32(requestPayload.size()).writeEncoder(requestPayload);
}
