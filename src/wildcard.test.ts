import { isWildcard, matchesTopic, matchesFilter } from './wildcard';

describe('isWildcard', () => {
  it.each([
    ['a/b/c', false],
    ['a/+/c', true],
    ['a/#', true],
    ['', false],
  ])('isWildcard(%p) === %p', (pattern, expected) => {
    expect(isWildcard(pattern)).toBe(expected);
  });
});

describe('matchesTopic', () => {
  it.each([
    // single-level '+'
    ['a/+/c', 'a/b/c', true],
    ['a/+/c', 'a/b/d', false],
    ['a/+/c', 'a/b', false], // length differs
    ['a/+', 'a', false], // '+' needs a segment
    ['a/+', 'a/b/c', false], // single-level, lengths differ
    ['a/+/+', 'a/b/c', true],
    // multi-level '#'
    ['a/#', 'a/b/c', true],
    ['a/#', 'a/b', true],
    ['a/#', 'a', true], // zero remaining levels
    ['a/b/#', 'a', false],
    // no wildcard -> exact equality
    ['a/b/c', 'a/b/c', true],
    ['a/b/c', 'a/b/d', false],
    // StatsPump-shaped, mirroring the Go tests
    [
      'mqtt/PUMP/solace1025/POLLER_STAT/VPN/+/queue_rates',
      'mqtt/PUMP/solace1025/POLLER_STAT/VPN/vpn3/queue_rates',
      true,
    ],
    [
      'mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/+',
      'mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/stats_client',
      true,
    ],
    [
      'mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/+',
      'mqtt/PUMP/solace1025/POLLER_STAT/VPN/queue',
      false,
    ],
  ])('matchesTopic(%p, %p) === %p', (pattern, topic, expected) => {
    expect(matchesTopic(pattern, topic)).toBe(expected);
  });
});

describe('matchesFilter', () => {
  it.each([
    ['host/two', '', true], // empty filter passes everything
    ['host/two', '!two', false], // exclude
    ['host/three', '!two', true], // exclude doesn't hit
    ['host/two', 'two,three', true], // include OR
    ['host/three', 'two,three', true],
    ['host/one', 'two,three', false], // no include matches
    ['host/two/edge', 'edge,!two', false], // include hits but exclude also hits
    ['host/three/edge', 'edge,!two', true], // include hits, exclude misses
    ['host/two', '!', true], // lone "!" is an empty exclude, ignored
    ['host/two', ' two , !nope ', true], // whitespace trimmed
    ['host/Two', 'two', false], // case-sensitive
  ])('matchesFilter(%p, %p) === %p', (topic, filter, expected) => {
    expect(matchesFilter(topic, filter)).toBe(expected);
  });
});
