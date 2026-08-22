import { describe, expect, it } from 'vitest';
import { murmur2 } from './murmur2';

const KNOWN_ANSWERS: Readonly<Record<string, number>> = {
  '0': 272173970,
  '1': 1311020360,
  '128': 2053105854,
  '2187': -2081355488,
  '16384': 204404061,
  '78125': -677491393,
  '279936': -622460209,
  '823543': 651276451,
  '2097152': 944683677,
  '4782969': -892695770,
  '10000000': -1778616326,
  '19487171': -518311627,
  '35831808': 556972389,
  '62748517': -233806557,
  '105413504': -109398538,
  '170859375': 102939717,
};

describe('producer/partitioners/legacy/murmur2', () => {
  it('matches pre-2.0.0 known-answer vectors, distinct from the default hash', () => {
    for (const [key, expected] of Object.entries(KNOWN_ANSWERS)) {
      expect(murmur2(key)).toBe(expected);
    }
  });

  it('handles numeric input', () => {
    expect(murmur2(0)).toBe(272173970);
  });

  it('hashes Buffer keys as raw bytes without String() conversion', () => {
    const key = Buffer.from([0xff, 0x00, 0xfe]);
    expect(murmur2(key)).not.toBe(murmur2(String(key)));
  });
});
