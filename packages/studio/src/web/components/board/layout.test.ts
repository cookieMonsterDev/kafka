import { describe, expect, it } from 'vitest';
import { findNode, layoutBoard, MAX_DISPLAYED_GROUPS, MAX_DISPLAYED_TOPICS, pathForTopic } from './layout';

function topic(name: string, partitionCount = 3) {
  return { name, partitionCount };
}

function group(groupId: string, protocolType = 'consumer') {
  return { groupId, protocolType };
}

describe('layoutBoard', () => {
  it('always includes the studio and cluster nodes', () => {
    const { nodes } = layoutBoard([], []);
    expect(nodes.map((node) => node.id)).toEqual(['studio', 'cluster']);
  });

  it('adds one node per topic and per group, with partition/protocol subtitles', () => {
    const { nodes } = layoutBoard([topic('orders', 2), topic('payments', 6)], [group('checkout')]);
    expect(nodes.map((node) => node.id)).toEqual([
      'studio',
      'cluster',
      'topic:orders',
      'topic:payments',
      'group:checkout',
    ]);
    expect(nodes.find((node) => node.id === 'topic:orders')?.subtitle).toBe('2 partitions');
    expect(nodes.find((node) => node.id === 'topic:payments')?.subtitle).toBe('6 partitions');
    expect(nodes.find((node) => node.id === 'group:checkout')?.subtitle).toBe('consumer');
  });

  it('uses the singular for a single partition', () => {
    const { nodes } = layoutBoard([topic('orders', 1)], []);
    expect(nodes.find((node) => node.id === 'topic:orders')?.subtitle).toBe('1 partition');
  });

  it('caps the number of displayed topics and groups', () => {
    const topics = Array.from({ length: MAX_DISPLAYED_TOPICS + 5 }, (_, i) => topic(`topic-${String(i)}`));
    const groups = Array.from({ length: MAX_DISPLAYED_GROUPS + 5 }, (_, i) => group(`group-${String(i)}`));
    const { nodes } = layoutBoard(topics, groups);

    expect(nodes.filter((node) => node.kind === 'topic')).toHaveLength(MAX_DISPLAYED_TOPICS);
    expect(nodes.filter((node) => node.kind === 'group')).toHaveLength(MAX_DISPLAYED_GROUPS);
  });

  it('connects every topic and group to the cluster hub, and the studio to the hub', () => {
    const { edges } = layoutBoard([topic('orders')], [group('checkout')]);
    expect(edges).toEqual([
      { id: 'studio-cluster', from: 'studio', to: 'cluster' },
      { id: 'cluster-topic:orders', from: 'cluster', to: 'topic:orders' },
      { id: 'cluster-group:checkout', from: 'cluster', to: 'group:checkout' },
    ]);
  });
});

describe('findNode', () => {
  it('finds a node by id', () => {
    const layout = layoutBoard([topic('orders')], []);
    expect(findNode(layout, 'topic:orders')?.label).toBe('orders');
  });

  it('returns undefined for an unknown id', () => {
    const layout = layoutBoard([], []);
    expect(findNode(layout, 'topic:missing')).toBeUndefined();
  });
});

describe('pathForTopic', () => {
  it('routes through the topic node when it is displayed on the board', () => {
    const layout = layoutBoard([topic('orders')], []);
    expect(pathForTopic(layout, 'orders')).toEqual(['studio', 'cluster', 'topic:orders']);
  });

  it('stops at the cluster hub when the topic was capped off the board', () => {
    const layout = layoutBoard([], []);
    expect(pathForTopic(layout, 'orders')).toEqual(['studio', 'cluster']);
  });
});
