/**
 * Feature gates for this package's integration suites. Their compose matrix
 * (`test/helpers/global-setup.ts`) only ever selects `0.10`, `4.0`, or `4.4`, so a feature that
 * needs a broker newer than 4.0 is available only on `4.4`.
 */
const KAFKA_VERSION = process.env.KAFKA_VERSION ?? '4.0';

/** Share groups (KIP-932) need a broker at least at 4.1. */
export const SUPPORTS_SHARE_GROUPS = KAFKA_VERSION === '4.4';

/** Streams groups (KIP-1071) need a broker at least at 4.2. */
export const SUPPORTS_STREAMS_GROUPS = KAFKA_VERSION === '4.4';
