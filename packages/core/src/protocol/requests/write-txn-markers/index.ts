import type { ProtocolFactory, RequestFamily } from '../index';
import { writeTxnMarkersRequestV1, type WriteTxnMarkersMarkerV1, type WriteTxnMarkersTopic } from './v1/request';
import { writeTxnMarkersResponseV1, type WriteTxnMarkersResponseV1Body } from './v1/response';
import { writeTxnMarkersRequestV2, type WriteTxnMarkersMarkerV2 } from './v2/request';
import { writeTxnMarkersResponseV2 } from './v2/response';

export type { WriteTxnMarkersTopic };

export type WriteTxnMarkersMarker = WriteTxnMarkersMarkerV1 & {
  transactionVersion?: number;
};

export interface WriteTxnMarkersOptions {
  markers: WriteTxnMarkersMarker[];
}

export type { WriteTxnMarkersResponseV1Body };

const toV1Marker = (marker: WriteTxnMarkersMarker): WriteTxnMarkersMarkerV1 => ({
  producerId: marker.producerId,
  producerEpoch: marker.producerEpoch,
  transactionResult: marker.transactionResult,
  coordinatorEpoch: marker.coordinatorEpoch,
  topics: marker.topics,
});

const toV2Marker = (marker: WriteTxnMarkersMarker): WriteTxnMarkersMarkerV2 => ({
  ...toV1Marker(marker),
  transactionVersion: marker.transactionVersion ?? 0,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<WriteTxnMarkersOptions>>> = {
  1: (options) => ({
    request: writeTxnMarkersRequestV1({ markers: options.markers.map(toV1Marker) }),
    response: writeTxnMarkersResponseV1,
  }),
  2: (options) => ({
    request: writeTxnMarkersRequestV2({ markers: options.markers.map(toV2Marker) }),
    response: writeTxnMarkersResponseV2,
  }),
};

export const WriteTxnMarkers: RequestFamily<WriteTxnMarkersOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no WriteTxnMarkers protocol for version ${version}`);
    return factory;
  },
});
