---
title: Benchmarks
description: Draft comparison of kafka-core with @platformatic/kafka, KafkaJS and Confluent's client on Node.js and Bun
order: 12
section: reference
---

How `@cookiemonsterdev/kafka-core` compares with `@platformatic/kafka` 2.15.0, `kafkajs` 2.2.4 and `@confluentinc/kafka-javascript` 1.11.0 on Node.js 24.22.3, Node.js 26.13.1 and Bun 1.4.4.

## How it was measured

Each client ran in a fresh Docker container with 2 vCPUs and 2 GiB, against a single `apache/kafka:4.3.2` broker in KRaft mode, on one Apple M5 laptop through Docker Desktop. Every value is the median of 5 runs, with the min–max range. Scenarios, settings and fairness rules are in the [benchmarks README](https://github.com/cookieMonsterDev/kafka/blob/develop/benchmarks/README.md).

## Read this first

- Only kafka-core verifies record-batch CRCs while consuming, so it does extra work in the consume and latency results.
- Concurrent sends favour clients that merge separate `send()` calls into one batch, as kafka-core does. Batched send is the fairer produce comparison.
- Confluent's client runs on Node.js 24 only. Some of its consume runs ran out of memory, so those medians cover the remaining runs.
- All results come from one machine. Treat small differences as inconclusive.

## Produce

### Batched send (100 records)

Each call awaits `send()` of 100 records with acks 1. This is the fairest produce comparison.

```chart
{
  "description": "Bar chart of batched send (100 records) by runtime and client, higher is better. Highest: Node.js 24: kafka-core (709.5k records/s), Node.js 26: kafka-core (717.8k records/s), Bun: kafka-core (752.5k records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [709513, 717837, 752482]
    },
    {
      "name": "Platformatic",
      "data": [446119, 521241, 536314]
    },
    {
      "name": "KafkaJS",
      "data": [289028, 308089, 291945]
    },
    {
      "name": "Confluent",
      "data": [191878, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 1000000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max         |
| ---------- | ------------ | ------------------ | --------------- |
| Node.js 24 | kafka-core   | 709,513            | 660,971–722,277 |
| Node.js 24 | Platformatic | 446,119            | 408,191–487,050 |
| Node.js 24 | KafkaJS      | 289,028            | 253,883–298,168 |
| Node.js 24 | Confluent    | 191,878            | 164,918–201,752 |
| Node.js 26 | kafka-core   | 717,837            | 584,659–757,205 |
| Node.js 26 | Platformatic | 521,241            | 510,802–531,145 |
| Node.js 26 | KafkaJS      | 308,089            | 297,019–318,283 |
| Node.js 26 | Confluent    | not supported      | —               |
| Bun        | kafka-core   | 752,482            | 693,580–869,905 |
| Bun        | Platformatic | 536,314            | 435,065–562,036 |
| Bun        | KafkaJS      | 291,945            | 228,216–325,018 |
| Bun        | Confluent    | not supported      | —               |

### Single-record send

Each call awaits `send()` of one record with acks 1 and no linger, so every record waits for a broker round trip.

```chart
{
  "description": "Bar chart of single-record send by runtime and client, higher is better. Highest: Node.js 24: Platformatic (13.9k records/s), Node.js 26: Platformatic (14.0k records/s), Bun: Platformatic (14.2k records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs. kafka-core: earlier build."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [12420, 12582, 12584]
    },
    {
      "name": "Platformatic",
      "data": [13927, 14015, 14238]
    },
    {
      "name": "KafkaJS",
      "data": [12324, 13206, 12647]
    },
    {
      "name": "Confluent",
      "data": [9662, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 20000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max       |
| ---------- | ------------ | ------------------ | ------------- |
| Node.js 24 | kafka-core   | 12,420             | 11,723–12,812 |
| Node.js 24 | Platformatic | 13,927             | 13,297–14,081 |
| Node.js 24 | KafkaJS      | 12,324             | 4,981–12,977  |
| Node.js 24 | Confluent    | 9,662              | 7,359–9,827   |
| Node.js 26 | kafka-core   | 12,582             | 11,652–12,753 |
| Node.js 26 | Platformatic | 14,015             | 13,345–14,215 |
| Node.js 26 | KafkaJS      | 13,206             | 10,321–13,282 |
| Node.js 26 | Confluent    | not supported      | —             |
| Bun        | kafka-core   | 12,584             | 2,190–13,868  |
| Bun        | Platformatic | 14,238             | 2,261–14,454  |
| Bun        | KafkaJS      | 12,647             | 8,029–13,244  |
| Bun        | Confluent    | not supported      | —             |

### Concurrent sends

Up to 256 concurrent single-record `send()` calls with linger 5 ms. This favours clients that merge separate calls into one batch, as kafka-core does.

```chart
{
  "description": "Bar chart of concurrent sends by runtime and client, higher is better. Highest: Node.js 24: kafka-core (452.9k records/s), Node.js 26: kafka-core (383.0k records/s), Bun: kafka-core (524.2k records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [452892, 383003, 524157]
    },
    {
      "name": "Platformatic",
      "data": [23416, 21684, 23319]
    },
    {
      "name": "KafkaJS",
      "data": [21665, 21849, 21108]
    },
    {
      "name": "Confluent",
      "data": [39003, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 700000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max         |
| ---------- | ------------ | ------------------ | --------------- |
| Node.js 24 | kafka-core   | 452,892            | 396,432–471,010 |
| Node.js 24 | Platformatic | 23,416             | 18,723–23,770   |
| Node.js 24 | KafkaJS      | 21,665             | 20,932–22,277   |
| Node.js 24 | Confluent    | 39,003             | 38,165–39,546   |
| Node.js 26 | kafka-core   | 383,003            | 341,123–460,147 |
| Node.js 26 | Platformatic | 21,684             | 19,297–23,212   |
| Node.js 26 | KafkaJS      | 21,849             | 14,333–21,923   |
| Node.js 26 | Confluent    | not supported      | —               |
| Bun        | kafka-core   | 524,157            | 446,088–553,226 |
| Bun        | Platformatic | 23,319             | 12,978–23,800   |
| Bun        | KafkaJS      | 21,108             | 20,575–21,377   |
| Bun        | Confluent    | not supported      | —               |

## Consume

### Per message

Reads 2,000,000 preloaded records with a per-record handler and the same fetch settings for every client.

```chart
{
  "description": "Bar chart of consume (per message) by runtime and client, higher is better. Highest: Node.js 24: kafka-core (2.84M records/s), Node.js 26: kafka-core (2.97M records/s), Bun: kafka-core (3.96M records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [2840138, 2970011, 3964604]
    },
    {
      "name": "Platformatic",
      "data": [2232234, 1830716, 2092284]
    },
    {
      "name": "KafkaJS",
      "data": [970691, 1174870, 1259417]
    },
    {
      "name": "Confluent",
      "data": [178120, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 5500000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max             |
| ---------- | ------------ | ------------------ | ------------------- |
| Node.js 24 | kafka-core   | 2,840,138          | 2,640,318–2,962,253 |
| Node.js 24 | Platformatic | 2,232,234          | 2,129,165–2,386,443 |
| Node.js 24 | KafkaJS      | 970,691            | 945,071–1,034,184   |
| Node.js 24 | Confluent    | 178,120            | 161,606–196,560     |
| Node.js 26 | kafka-core   | 2,970,011          | 2,602,785–3,112,933 |
| Node.js 26 | Platformatic | 1,830,716          | 1,684,293–1,902,242 |
| Node.js 26 | KafkaJS      | 1,174,870          | 1,026,101–1,190,249 |
| Node.js 26 | Confluent    | not supported      | —                   |
| Bun        | kafka-core   | 3,964,604          | 3,790,672–4,180,539 |
| Bun        | Platformatic | 2,092,284          | 1,946,254–2,180,982 |
| Bun        | KafkaJS      | 1,259,417          | 873,166–1,330,196   |
| Bun        | Confluent    | not supported      | —                   |

### Per message, library defaults

The same read with each library's default settings, including its own auto-commit.

```chart
{
  "description": "Bar chart of consume (per message, library defaults) by runtime and client, higher is better. Highest: Node.js 24: KafkaJS (824.6k records/s), Node.js 26: KafkaJS (945.7k records/s), Bun: Platformatic (1.42M records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [126747, 183445, 361618]
    },
    {
      "name": "Platformatic",
      "data": [424423, 525024, 1420916]
    },
    {
      "name": "KafkaJS",
      "data": [824614, 945732, 1136689]
    },
    {
      "name": "Confluent",
      "data": [313186, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 2000000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max             |
| ---------- | ------------ | ------------------ | ------------------- |
| Node.js 24 | kafka-core   | 126,747            | 43,656–346,826      |
| Node.js 24 | Platformatic | 424,423            | 405,165–440,681     |
| Node.js 24 | KafkaJS      | 824,614            | 821,046–929,030     |
| Node.js 24 | Confluent    | 313,186            | 303,868–319,186     |
| Node.js 26 | kafka-core   | 183,445            | 49,018–350,867      |
| Node.js 26 | Platformatic | 525,024            | 499,509–542,006     |
| Node.js 26 | KafkaJS      | 945,732            | 712,067–978,535     |
| Node.js 26 | Confluent    | not supported      | —                   |
| Bun        | kafka-core   | 361,618            | 189,767–4,122,732   |
| Bun        | Platformatic | 1,420,916          | 1,229,162–1,472,680 |
| Bun        | KafkaJS      | 1,136,689          | 1,005,256–1,156,530 |
| Bun        | Confluent    | not supported      | —                   |

### Batch

The same read through each client's batch API. Platformatic has none, so its adapter emulates one.

```chart
{
  "description": "Bar chart of consume (batch) by runtime and client, higher is better. Highest: Node.js 24: kafka-core (3.56M records/s), Node.js 26: kafka-core (3.49M records/s), Bun: kafka-core (5.47M records/s). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Higher is better (records/s)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [3561503, 3487168, 5474426]
    },
    {
      "name": "Platformatic",
      "data": [1691424, 1732593, 1951101]
    },
    {
      "name": "KafkaJS",
      "data": [1725529, 2094057, 2257846]
    },
    {
      "name": "Confluent",
      "data": [244047, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "records per second"
    },
    "max": 7500000
  }
}
```

| Runtime    | Client       | Median (records/s) | Min–max             |
| ---------- | ------------ | ------------------ | ------------------- |
| Node.js 24 | kafka-core   | 3,561,503          | 2,962,334–4,329,863 |
| Node.js 24 | Platformatic | 1,691,424          | 1,505,586–1,803,776 |
| Node.js 24 | KafkaJS      | 1,725,529          | 1,426,980–1,852,559 |
| Node.js 24 | Confluent    | 244,047            | 233,090–255,003     |
| Node.js 26 | kafka-core   | 3,487,168          | 2,735,979–3,562,164 |
| Node.js 26 | Platformatic | 1,732,593          | 1,556,407–1,795,269 |
| Node.js 26 | KafkaJS      | 2,094,057          | 1,786,770–2,230,936 |
| Node.js 26 | Confluent    | not supported      | —                   |
| Bun        | kafka-core   | 5,474,426          | 4,819,900–5,652,237 |
| Bun        | Platformatic | 1,951,101          | 1,818,107–1,979,417 |
| Bun        | KafkaJS      | 2,257,846          | 2,093,900–2,294,783 |
| Bun        | Confluent    | not supported      | —                   |

## End-to-end latency

### Median

A producer sends 5,000 records per second for 15 seconds while a consumer reads them. Latency runs from the scheduled send time to consumption.

```chart
{
  "description": "Bar chart of end-to-end latency, median by runtime and client, lower is better. Lowest: Node.js 24: kafka-core (1.45 ms), Node.js 26: Platformatic (1.46 ms), Bun: kafka-core (1.02 ms). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (ms)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [1.452, 1.559, 1.025]
    },
    {
      "name": "Platformatic",
      "data": [1.462, 1.463, 1.158]
    },
    {
      "name": "KafkaJS",
      "data": [1.5, 1.487, 1.034]
    },
    {
      "name": "Confluent",
      "data": [3.363, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "milliseconds"
    },
    "max": 4.5
  }
}
```

| Runtime    | Client       | Median (ms)   | Min–max   |
| ---------- | ------------ | ------------- | --------- |
| Node.js 24 | kafka-core   | 1.45          | 1.38–1.61 |
| Node.js 24 | Platformatic | 1.46          | 1.46–1.65 |
| Node.js 24 | KafkaJS      | 1.50          | 1.48–1.70 |
| Node.js 24 | Confluent    | 3.36          | 3.31–3.45 |
| Node.js 26 | kafka-core   | 1.56          | 1.46–1.73 |
| Node.js 26 | Platformatic | 1.46          | 1.41–1.57 |
| Node.js 26 | KafkaJS      | 1.49          | 1.45–1.70 |
| Node.js 26 | Confluent    | not supported | —         |
| Bun        | kafka-core   | 1.02          | 1.01–1.04 |
| Bun        | Platformatic | 1.16          | 1.05–1.19 |
| Bun        | KafkaJS      | 1.03          | 1.03–1.08 |
| Bun        | Confluent    | not supported | —         |

### 99th percentile

The same runs, 99th percentile.

```chart
{
  "description": "Bar chart of end-to-end latency, p99 by runtime and client, lower is better. Lowest: Node.js 24: KafkaJS (5.28 ms), Node.js 26: KafkaJS (3.48 ms), Bun: KafkaJS (3.14 ms). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (ms)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [14.977, 6.665, 3.409]
    },
    {
      "name": "Platformatic",
      "data": [5.711, 5.858, 3.629]
    },
    {
      "name": "KafkaJS",
      "data": [5.281, 3.48, 3.137]
    },
    {
      "name": "Confluent",
      "data": [8.139, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "milliseconds"
    },
    "max": 20
  }
}
```

| Runtime    | Client       | Median (ms)   | Min–max    |
| ---------- | ------------ | ------------- | ---------- |
| Node.js 24 | kafka-core   | 14.98         | 3.18–83.09 |
| Node.js 24 | Platformatic | 5.71          | 4.17–70.26 |
| Node.js 24 | KafkaJS      | 5.28          | 3.27–37.71 |
| Node.js 24 | Confluent    | 8.14          | 7.95–9.35  |
| Node.js 26 | kafka-core   | 6.67          | 3.61–14.93 |
| Node.js 26 | Platformatic | 5.86          | 3.41–12.65 |
| Node.js 26 | KafkaJS      | 3.48          | 2.95–8.02  |
| Node.js 26 | Confluent    | not supported | —          |
| Bun        | kafka-core   | 3.41          | 3.05–23.10 |
| Bun        | Platformatic | 3.63          | 3.15–5.13  |
| Bun        | KafkaJS      | 3.14          | 2.67–4.15  |
| Bun        | Confluent    | not supported | —          |

## Startup

Time from process start to the first acknowledged produce.

```chart
{
  "description": "Bar chart of startup, first acknowledged produce by runtime and client, lower is better. Lowest: Node.js 24: KafkaJS (91 ms), Node.js 26: KafkaJS (83 ms), Bun: KafkaJS (43 ms). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (ms)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs. kafka-core: earlier build."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [159.7, 147, 53.2]
    },
    {
      "name": "Platformatic",
      "data": [203.2, 180.8, 124.1]
    },
    {
      "name": "KafkaJS",
      "data": [90.6, 82.8, 43.1]
    },
    {
      "name": "Confluent",
      "data": [229.5, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "milliseconds from process start"
    },
    "max": 300
  }
}
```

| Runtime    | Client       | Median (ms)   | Min–max     |
| ---------- | ------------ | ------------- | ----------- |
| Node.js 24 | kafka-core   | 159.7         | 151.2–180.7 |
| Node.js 24 | Platformatic | 203.2         | 190.5–258.8 |
| Node.js 24 | KafkaJS      | 90.6          | 84.2–104.0  |
| Node.js 24 | Confluent    | 229.5         | 219.0–254.7 |
| Node.js 26 | kafka-core   | 147.0         | 143.4–162.3 |
| Node.js 26 | Platformatic | 180.8         | 172.8–211.0 |
| Node.js 26 | KafkaJS      | 82.8          | 79.0–118.7  |
| Node.js 26 | Confluent    | not supported | —           |
| Bun        | kafka-core   | 53.2          | 51.5–83.4   |
| Bun        | Platformatic | 124.1         | 116.4–206.7 |
| Bun        | KafkaJS      | 43.1          | 34.2–47.0   |
| Bun        | Confluent    | not supported | —           |

## Peak memory

### Batched send

Peak container memory during batched send, under a 2 GiB limit.

```chart
{
  "description": "Bar chart of peak memory, batched send by runtime and client, lower is better. Lowest: Node.js 24: Confluent (66 MiB), Node.js 26: KafkaJS (89 MiB), Bun: KafkaJS (58 MiB). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (MiB)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [104, 99.32, 63.91]
    },
    {
      "name": "Platformatic",
      "data": [108.62, 96.05, 84.25]
    },
    {
      "name": "KafkaJS",
      "data": [88.46, 89.26, 57.64]
    },
    {
      "name": "Confluent",
      "data": [66.39, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "MiB (peak container memory)"
    },
    "max": 150
  }
}
```

| Runtime    | Client       | Median (MiB)  | Min–max |
| ---------- | ------------ | ------------- | ------- |
| Node.js 24 | kafka-core   | 104           | 101–109 |
| Node.js 24 | Platformatic | 109           | 101–113 |
| Node.js 24 | KafkaJS      | 88            | 86–97   |
| Node.js 24 | Confluent    | 66            | 59–70   |
| Node.js 26 | kafka-core   | 99            | 92–103  |
| Node.js 26 | Platformatic | 96            | 91–103  |
| Node.js 26 | KafkaJS      | 89            | 86–93   |
| Node.js 26 | Confluent    | not supported | —       |
| Bun        | kafka-core   | 64            | 64–65   |
| Bun        | Platformatic | 84            | 82–87   |
| Bun        | KafkaJS      | 58            | 55–64   |
| Bun        | Confluent    | not supported | —       |

### Consume (per message)

Peak container memory while consuming per message, under a 2 GiB limit.

```chart
{
  "description": "Bar chart of peak memory, consume (per message) by runtime and client, lower is better. Lowest: Node.js 24: kafka-core (211 MiB), Node.js 26: KafkaJS (158 MiB), Bun: kafka-core (136 MiB). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (MiB)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [211.25, 179.48, 135.66]
    },
    {
      "name": "Platformatic",
      "data": [221.32, 210.89, 167.98]
    },
    {
      "name": "KafkaJS",
      "data": [220.73, 157.67, 153.02]
    },
    {
      "name": "Confluent",
      "data": [1751.74, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "MiB (peak container memory)"
    },
    "max": 2500
  }
}
```

| Runtime    | Client       | Median (MiB)  | Min–max     |
| ---------- | ------------ | ------------- | ----------- |
| Node.js 24 | kafka-core   | 211           | 196–214     |
| Node.js 24 | Platformatic | 221           | 219–230     |
| Node.js 24 | KafkaJS      | 221           | 208–227     |
| Node.js 24 | Confluent    | 1,752         | 1,159–2,029 |
| Node.js 26 | kafka-core   | 179           | 171–276     |
| Node.js 26 | Platformatic | 211           | 195–215     |
| Node.js 26 | KafkaJS      | 158           | 155–165     |
| Node.js 26 | Confluent    | not supported | —           |
| Bun        | kafka-core   | 136           | 123–140     |
| Bun        | Platformatic | 168           | 165–181     |
| Bun        | KafkaJS      | 153           | 130–160     |
| Bun        | Confluent    | not supported | —           |

### Consume (batch)

Peak container memory while consuming in batches, under a 2 GiB limit.

```chart
{
  "description": "Bar chart of peak memory, consume (batch) by runtime and client, lower is better. Lowest: Node.js 24: KafkaJS (210 MiB), Node.js 26: KafkaJS (161 MiB), Bun: kafka-core (131 MiB). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 420
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (MiB)"
  },
  "subtitle": {
    "text": "Draft. Median of 5 runs."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [351.73, 197.52, 130.59]
    },
    {
      "name": "Platformatic",
      "data": [243.01, 225.93, 173.36]
    },
    {
      "name": "KafkaJS",
      "data": [209.8, 161.39, 140.48]
    },
    {
      "name": "Confluent",
      "data": [1264.77, null, null]
    }
  ],
  "xaxis": {
    "categories": ["Node.js 24", "Node.js 26", "Bun"],
    "title": {
      "text": "MiB (peak container memory)"
    },
    "max": 2000
  }
}
```

| Runtime    | Client       | Median (MiB)  | Min–max     |
| ---------- | ------------ | ------------- | ----------- |
| Node.js 24 | kafka-core   | 352           | 231–405     |
| Node.js 24 | Platformatic | 243           | 236–249     |
| Node.js 24 | KafkaJS      | 210           | 206–218     |
| Node.js 24 | Confluent    | 1,265         | 1,022–1,508 |
| Node.js 26 | kafka-core   | 198           | 181–312     |
| Node.js 26 | Platformatic | 226           | 221–227     |
| Node.js 26 | KafkaJS      | 161           | 155–166     |
| Node.js 26 | Confluent    | not supported | —           |
| Bun        | kafka-core   | 131           | 126–143     |
| Bun        | Platformatic | 173           | 167–191     |
| Bun        | KafkaJS      | 140           | 139–148     |
| Bun        | Confluent    | not supported | —           |

## Install size

Size of the package and its production dependencies as installed.

```chart
{
  "description": "Bar chart of install size by client, lower is better. Lowest: KafkaJS (0.7 MB). The table below lists every value.",
  "chart": {
    "type": "bar",
    "height": 240
  },
  "plotOptions": {
    "bar": {
      "horizontal": true,
      "barHeight": "85%"
    }
  },
  "title": {
    "text": "Lower is better (MB)"
  },
  "subtitle": {
    "text": "Draft. One measurement per client."
  },
  "series": [
    {
      "name": "kafka-core",
      "data": [5.12]
    },
    {
      "name": "Platformatic",
      "data": [9.08]
    },
    {
      "name": "KafkaJS",
      "data": [0.73]
    },
    {
      "name": "Confluent",
      "data": [47.23]
    }
  ],
  "xaxis": {
    "categories": ["Install size"],
    "title": {
      "text": "megabytes (1 MB = 1,000,000 bytes)"
    },
    "max": 65
  }
}
```

| Client       | Install size (MB) |
| ------------ | ----------------- |
| kafka-core   | 5.12              |
| Platformatic | 9.08              |
| KafkaJS      | 0.73              |
| Confluent    | 47.23             |

## How to reproduce

From the repository root, with Docker running:

```sh
pnpm benchmarks:run --tier standard --seed 3220318854
pnpm docs:benchmarks --competitors benchmarks/results/raw/<session> --core benchmarks/results/raw/<session>
```

The first command measures every client and writes a session directory; the second regenerates this page and its charts from it.
