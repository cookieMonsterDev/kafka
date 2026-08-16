import { describe, expect, it } from 'vitest';
import { murmur2 } from './murmur2.js';

// Generated with kafkajs's src/producer/partitioners/defaultJava/Test.java
const KNOWN_ANSWERS: Readonly<Record<string, number>> = {
  '0': 971027396,
  '1': -1993445489,
  '128': -326012175,
  '2187': -1508407203,
  '16384': -325739742,
  '78125': -1654490814,
  '279936': 1462227128,
  '823543': -2014198330,
  '2097152': 607668903,
  '4782969': -1182699775,
  '10000000': -1830336757,
  '19487171': -1603849305,
  '35831808': -857013643,
  '62748517': -1167431028,
  '105413504': -381294639,
  '170859375': -1658323481,
  '100:48069': 1009543857,
};

describe('producer/partitioners/default/murmur2', () => {
  it('matches the reference Java client hash for known keys', () => {
    for (const [key, expected] of Object.entries(KNOWN_ANSWERS)) {
      expect(murmur2(key)).toBe(expected);
    }
  });

  it('handles numeric input', () => {
    expect(murmur2(0)).toBe(971027396);
  });

  it('handles buffer input', () => {
    expect(murmur2(Buffer.from('1'))).toBe(-1993445489);
  });
});
