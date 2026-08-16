/**
 * SCRAM mechanism codes used by Describe/AlterUserScramCredentials (KIP-554).
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const SCRAM_MECHANISMS = Object.freeze({
  UNKNOWN: 0,
  SCRAM_SHA_256: 1,
  SCRAM_SHA_512: 2,
});

export type ScramMechanism = (typeof SCRAM_MECHANISMS)[keyof typeof SCRAM_MECHANISMS];
