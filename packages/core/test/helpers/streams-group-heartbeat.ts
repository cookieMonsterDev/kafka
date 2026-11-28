import { connect, type Socket } from 'node:net';
import { COORDINATOR_LOADING_ERRORS, STALE_COORDINATOR_ERRORS } from '../../src/cluster/error-classifier';
import type { Cluster } from '../../src/cluster/index';
import { Decoder } from '../../src/protocol/decoder';
import { Encoder } from '../../src/protocol/encoder';
import { createErrorFromCode } from '../../src/protocol/error-codes';
import { waitFor } from '../../src/utils/wait';

const STREAMS_GROUP_HEARTBEAT_API_KEY = 88;
const CORRELATION_ID = 1;

export interface StreamsGroupHeartbeatResult {
  errorCode: number;
  errorMessage: string | null;
  memberId: string;
  memberEpoch: number;
}

/** A join (member epoch 0) for a topology of one subtopology `'0'` that reads `sourceTopic`. */
function encodeJoinRequest({
  groupId,
  memberId,
  processId,
  sourceTopic,
}: {
  groupId: string;
  memberId: string;
  processId: string;
  sourceTopic: string;
}): Buffer {
  const nullArray = 0;
  const emptyArray = 1;
  const emptyTaggedFields = 0;
  const body = new Encoder()
    .writeUVarIntString(groupId)
    .writeUVarIntString(memberId)
    .writeInt32(0) // member epoch: join
    .writeInt32(0) // endpoint information epoch
    .writeUVarIntString(null) // instance id
    .writeUVarIntString(null) // rack id
    .writeInt32(30_000) // rebalance timeout
    .writeInt8(1) // topology: present
    .writeInt32(0) // topology epoch
    .writeUVarInt(2) // one subtopology
    .writeUVarIntString('0')
    .writeUVarInt(2)
    .writeUVarIntString(sourceTopic)
    .writeUVarInt(emptyArray) // source topic regexes
    .writeUVarInt(emptyArray) // state changelog topics
    .writeUVarInt(emptyArray) // repartition sink topics
    .writeUVarInt(emptyArray) // repartition source topics
    .writeUVarInt(emptyArray) // copartition groups
    .writeUVarInt(emptyTaggedFields)
    .writeUVarInt(emptyTaggedFields)
    .writeUVarInt(emptyArray) // active tasks
    .writeUVarInt(emptyArray) // standby tasks
    .writeUVarInt(emptyArray) // warm-up tasks
    .writeUVarIntString(processId)
    .writeInt8(-1) // user endpoint: none
    .writeUVarInt(emptyArray) // client tags
    .writeUVarInt(nullArray) // task offsets: Kafka 4.2 and 4.3 reject any, even an empty list
    .writeUVarInt(nullArray) // task end offsets
    .writeBoolean(false) // shutdown application
    .writeUVarInt(emptyTaggedFields);
  // Request header v2: api key, api version, correlation id, client id, tagged fields.
  const request = new Encoder()
    .writeInt16(STREAMS_GROUP_HEARTBEAT_API_KEY)
    .writeInt16(0)
    .writeInt32(CORRELATION_ID)
    .writeString('streams-group-test-helper')
    .writeUVarInt(emptyTaggedFields)
    .writeBuffer(body.buffer);
  return new Encoder().writeInt32(request.buffer.length).writeBuffer(request.buffer).buffer;
}

function readResponse(socket: Socket): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let received = Buffer.alloc(0);
    socket.on('data', (chunk: Buffer) => {
      received = Buffer.concat([received, chunk]);
      if (received.length >= 4 && received.length >= 4 + received.readInt32BE(0)) {
        resolve(received.subarray(4, 4 + received.readInt32BE(0)));
      }
    });
    socket.once('error', reject);
    socket.once('close', () => reject(new Error('The broker closed the connection before it answered')));
  });
}

async function sendJoinRequest(
  coordinatorAddress: string,
  request: Parameters<typeof encodeJoinRequest>[0],
): Promise<StreamsGroupHeartbeatResult> {
  const separator = coordinatorAddress.lastIndexOf(':');
  const socket = connect({
    host: coordinatorAddress.slice(0, separator),
    port: Number(coordinatorAddress.slice(separator + 1)),
  });
  try {
    const response = readResponse(socket);
    socket.write(encodeJoinRequest(request));
    // Response header v1: correlation id, tagged fields. The fields after the member epoch are not needed.
    const decoder = new Decoder(await response);
    decoder.readInt32();
    decoder.readTaggedFields();
    decoder.readInt32(); // throttle time
    return {
      errorCode: decoder.readInt16(),
      errorMessage: decoder.readUVarIntString(),
      memberId: decoder.readUVarIntString() ?? '',
      memberEpoch: decoder.readInt32(),
    };
  } finally {
    socket.destroy();
  }
}

/**
 * Joins a streams group as one member by sending a raw StreamsGroupHeartbeat v0 to the group's
 * coordinator, so integration tests can describe a real streams group. The client implements no
 * StreamsGroupHeartbeat (a Kafka Streams runtime sends it), so this frames the request by hand.
 * Retries while the coordinator is loading or has moved, as on a fresh cluster. The member stays
 * in the group until its session times out.
 */
export async function joinStreamsGroup({
  cluster,
  ...request
}: Parameters<typeof encodeJoinRequest>[0] & { cluster: Cluster }): Promise<StreamsGroupHeartbeatResult> {
  return waitFor(
    async () => {
      const coordinator = await cluster.findGroupCoordinator({ groupId: request.groupId });
      const result = await sendJoinRequest(coordinator.brokerAddress, request);
      const errorType = createErrorFromCode(result.errorCode).type ?? '';
      const isCoordinatorUnready = STALE_COORDINATOR_ERRORS.has(errorType) || COORDINATOR_LOADING_ERRORS.has(errorType);
      return isCoordinatorUnready ? false : result;
    },
    { delay: 200, maxWait: 30_000, timeoutMessage: 'The streams group coordinator did not become available' },
  );
}
