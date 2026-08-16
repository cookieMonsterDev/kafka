import type { TopicPartitions } from './types.js';

interface TopicState {
  topic: string;
  paused: Set<number>;
  pauseAll: boolean;
  resumed: Set<number>;
}

function createState(topic: string): TopicState {
  return {
    topic,
    paused: new Set(),
    pauseAll: false,
    resumed: new Set(),
  };
}

export class SubscriptionState {
  assignedPartitionsByTopic: Record<string, TopicPartitions> = {};
  subscriptionStatesByTopic: Record<string, TopicState> = {};

  assign(topicPartitions: readonly TopicPartitions[] = []): void {
    this.assignedPartitionsByTopic = topicPartitions.reduce<Record<string, TopicPartitions>>(
      (assigned, { topic, partitions = [] }) => {
        return { ...assigned, [topic]: { topic, partitions } };
      },
      {},
    );
  }

  pause(topicPartitions: readonly { topic: string; partitions?: number[] }[] = []): void {
    for (const { topic, partitions } of topicPartitions) {
      const state = this.subscriptionStatesByTopic[topic] ?? createState(topic);

      if (partitions === undefined) {
        state.paused.clear();
        state.resumed.clear();
        state.pauseAll = true;
      } else {
        for (const partition of partitions) {
          state.paused.add(partition);
          state.resumed.delete(partition);
        }
        state.pauseAll = false;
      }

      this.subscriptionStatesByTopic[topic] = state;
    }
  }

  resume(topicPartitions: readonly { topic: string; partitions?: number[] }[] = []): void {
    for (const { topic, partitions } of topicPartitions) {
      const state = this.subscriptionStatesByTopic[topic] ?? createState(topic);

      if (partitions === undefined) {
        state.paused.clear();
        state.resumed.clear();
        state.pauseAll = false;
      } else {
        for (const partition of partitions) {
          state.paused.delete(partition);
          if (state.pauseAll) {
            state.resumed.add(partition);
          }
        }
      }

      this.subscriptionStatesByTopic[topic] = state;
    }
  }

  assigned(): TopicPartitions[] {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => ({
      topic,
      partitions: [...partitions].sort((a, b) => a - b),
    }));
  }

  active(): TopicPartitions[] {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter((partition) => !this.isPaused(topic, partition)).sort((a, b) => a - b),
    }));
  }

  paused(): TopicPartitions[] {
    return Object.values(this.assignedPartitionsByTopic)
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter((partition) => this.isPaused(topic, partition)).sort((a, b) => a - b),
      }))
      .filter(({ partitions }) => partitions.length !== 0);
  }

  isPaused(topic: string, partition: number): boolean {
    const state = this.subscriptionStatesByTopic[topic];
    if (!state) return false;

    const partitionResumed = state.resumed.has(partition);
    const partitionPaused = state.paused.has(partition);
    return (state.pauseAll && !partitionResumed) || partitionPaused;
  }
}
