/**
 * The board's coordinate system is a fixed virtual box, not pixels — both the SVG skeleton and the
 * canvas particle layer position everything in this space, then a single shared transform (pan +
 * zoom + the box's current on-screen size) maps it to real pixels. That is what keeps the two
 * layers aligned after a resize or a zoom: neither one measures the DOM on its own, they both read
 * the same numbers.
 */
export const VIEWBOX_WIDTH = 1100;
export const VIEWBOX_HEIGHT = 640;

export type BoardNodeKind = 'studio' | 'cluster' | 'topic' | 'group';

export interface BoardNode {
  readonly id: string;
  readonly kind: BoardNodeKind;
  readonly label: string;
  /** A short fact under the label — partition count for a topic, protocol for a group. Omitted where there's nothing real to show. */
  readonly subtitle?: string;
  readonly x: number;
  readonly y: number;
}

export interface BoardEdge {
  readonly id: string;
  readonly from: string;
  readonly to: string;
}

export interface BoardLayout {
  readonly nodes: readonly BoardNode[];
  readonly edges: readonly BoardEdge[];
}

export interface BoardTopicInput {
  readonly name: string;
  readonly partitionCount: number;
}

export interface BoardGroupInput {
  readonly groupId: string;
  readonly protocolType: string;
}

/** More than this many topics or groups would overlap at this canvas size — the rest are still counted in the rail, just not drawn as nodes. */
export const MAX_DISPLAYED_TOPICS = 10;
export const MAX_DISPLAYED_GROUPS = 8;

/** The node card's rendered size — shared with `topology.tsx` so column positions can leave enough margin that a card never clips against the viewBox edge. */
export const CARD_WIDTH = 208;
export const CARD_HEIGHT = 68;
const EDGE_MARGIN = CARD_WIDTH / 2 + 24;

const CLUSTER_Y = VIEWBOX_HEIGHT / 2;
const STUDIO_X = EDGE_MARGIN;
const CLUSTER_X = VIEWBOX_WIDTH * 0.32;
const TOPIC_X = VIEWBOX_WIDTH * 0.62;
const GROUP_X = VIEWBOX_WIDTH - EDGE_MARGIN;

/** A fixed gap between rows, not "stretch across the whole canvas" — a couple of nodes cluster near the hub's own height instead of a long, sparse-looking line to a node pinned near the very top or bottom. */
const ROW_GAP = 108;
const MAX_SPAN = VIEWBOX_HEIGHT * 0.72;

function evenlySpacedY(count: number, index: number): number {
  if (count <= 1) return CLUSTER_Y;
  const span = Math.min(ROW_GAP * (count - 1), MAX_SPAN);
  const start = CLUSTER_Y - span / 2;
  return start + (span * index) / (count - 1);
}

function partitionSubtitle(count: number): string {
  return `${String(count)} partition${count === 1 ? '' : 's'}`;
}

/**
 * Builds the fixed topology: one Studio node and one cluster hub, a column of topic nodes and a
 * column of consumer-group nodes, both real (sourced from the topic and group lists this route
 * already fetches) and both capped so the board stays legible on a large cluster.
 */
export function layoutBoard(
  topicInputs: readonly BoardTopicInput[],
  groupInputs: readonly BoardGroupInput[],
): BoardLayout {
  const topics = topicInputs.slice(0, MAX_DISPLAYED_TOPICS);
  const groups = groupInputs.slice(0, MAX_DISPLAYED_GROUPS);

  const nodes: BoardNode[] = [
    { id: 'studio', kind: 'studio', label: 'This studio', x: STUDIO_X, y: CLUSTER_Y },
    { id: 'cluster', kind: 'cluster', label: 'Kafka cluster', x: CLUSTER_X, y: CLUSTER_Y },
    ...topics.map((topic, index): BoardNode => ({
      id: `topic:${topic.name}`,
      kind: 'topic',
      label: topic.name,
      subtitle: partitionSubtitle(topic.partitionCount),
      x: TOPIC_X,
      y: evenlySpacedY(topics.length, index),
    })),
    ...groups.map((group, index): BoardNode => ({
      id: `group:${group.groupId}`,
      kind: 'group',
      label: group.groupId,
      subtitle: group.protocolType,
      x: GROUP_X,
      y: evenlySpacedY(groups.length, index),
    })),
  ];

  const edges: BoardEdge[] = [
    { id: 'studio-cluster', from: 'studio', to: 'cluster' },
    ...topics.map((topic): BoardEdge => ({
      id: `cluster-topic:${topic.name}`,
      from: 'cluster',
      to: `topic:${topic.name}`,
    })),
    ...groups.map((group): BoardEdge => ({
      id: `cluster-group:${group.groupId}`,
      from: 'cluster',
      to: `group:${group.groupId}`,
    })),
  ];

  return { nodes, edges };
}

export function findNode(layout: BoardLayout, id: string): BoardNode | undefined {
  return layout.nodes.find((node) => node.id === id);
}

/**
 * The edge a produce/consume event travels: Studio↔Cluster↔the event's topic (when that topic has
 * a node on the board), or just Studio↔Cluster otherwise — traffic on a topic the board had to
 * cap out of the topic column is still real, so it still animates as far as the cluster hub.
 */
export function pathForTopic(layout: BoardLayout, topic: string): readonly string[] {
  const topicNodeId = `topic:${topic}`;
  return findNode(layout, topicNodeId) !== undefined ? ['studio', 'cluster', topicNodeId] : ['studio', 'cluster'];
}
