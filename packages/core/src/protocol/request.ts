import { Encoder } from './encoder';

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
 * Wraps an already-encoded request body with the shared Kafka request header
 * (api key, api version, correlation id, client id) and the leading length prefix every
 * request/response on the wire carries.
 */
export async function createRequest({ correlationId, clientId, request }: CreateRequestOptions): Promise<Encoder> {
  const payload = await request.encode();
  const requestPayload = new Encoder()
    .writeInt16(request.apiKey)
    .writeInt16(request.apiVersion)
    .writeInt32(correlationId)
    .writeString(clientId)
    .writeEncoder(payload);

  return new Encoder().writeInt32(requestPayload.size()).writeEncoder(requestPayload);
}
