const UINT32_RANGE = 2 ** 32;

/** Words drawn per Web Crypto call, so most calls only read the buffer. */
export const RANDOM_WORDS_PER_BATCH = 256;

const randomWords = new Uint32Array(RANDOM_WORDS_PER_BATCH);
let nextWordIndex = RANDOM_WORDS_PER_BATCH;

/**
 * Returns a random number in [0, 1), like `Math.random()`, but from Web Crypto. Shuffled values end
 * up in request options, where static analysis treats `Math.random()` as insecure randomness. Node.js
 * and Bun expose Web Crypto globally, so importing the client does not load `node:crypto`.
 */
export function randomFraction(): number {
  if (nextWordIndex === RANDOM_WORDS_PER_BATCH) {
    globalThis.crypto.getRandomValues(randomWords);
    nextWordIndex = 0;
  }

  // In range: the index is reset above once it reaches the batch length.
  const word = randomWords[nextWordIndex] as number;
  nextWordIndex += 1;
  return word / UINT32_RANGE;
}
