import { Encoder } from './encoder';
import { usesFlexibleRequestHeader } from './flexible';

export interface CreateRequestOptions {
  correlationId: number;
  clientId: string;
  request: {
    apiKey: number;
    apiVersion: number;
    encode(): Encoder | Promise<Encoder>;
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
 * Returns a promise only when `encode` does, so a synchronous body (such as an uncompressed
 * Produce) is framed without a promise hop. A synchronous `encode` failure is thrown.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function createRequest({ correlationId, clientId, request }: CreateRequestOptions): Encoder | Promise<Encoder> {
  const payload = request.encode();
  if (payload instanceof Promise) {
    return payload.then((body) => frameRequest(correlationId, clientId, request, body));
  }
  return frameRequest(correlationId, clientId, request, payload);
}

function frameRequest(
  correlationId: number,
  clientId: string,
  request: CreateRequestOptions['request'],
  payload: Encoder,
): Encoder {
  const estimatedSize = 4 + 2 + 2 + 4 + 2 + Buffer.byteLength(clientId) + 1 + payload.size();
  const encoder = new Encoder(estimatedSize);

  encoder.writeInt32(0);
  encoder.writeInt16(request.apiKey).writeInt16(request.apiVersion).writeInt32(correlationId).writeString(clientId);

  if (usesFlexibleRequestHeader(request.apiKey, request.apiVersion)) {
    encoder.writeUVarInt(0);
  }

  // `payload` has no further use once its bytes are copied in — pool its buffer.
  encoder.writeEncoder(payload, { release: true });
  encoder.writeInt32At(0, encoder.size() - 4);
  return encoder;
}
