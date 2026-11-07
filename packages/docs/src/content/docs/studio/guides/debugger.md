---
title: Debug event flows
description: Search and correlate events, replay failures, inspect compacted state, use schemas, and run repeatable scenarios
order: 2
section: guides
---

Open **Debugger** from the sidebar or command palette. Operations use the active profile,
short-lived readers, and the existing producer. They never commit consumer-group offsets.
Changing profiles cancels the browser's operation and clears its results and drafts. Changing
Debugger modes keeps drafts. Replay and scenario execution require a writable Studio; the
server also rejects direct HTTP writes in read-only mode.

## Search and correlate

Date-time fields use a calendar popover and separate 24-hour/minute selectors. **Now** fills the
current local time, **Clear** removes the filter, and **Done** closes the picker.

Enter up to eight comma-separated topic names. **Scan snapshot** captures partition high
watermarks and reads retained history, optionally starting at a local date/time. The through-time
filter includes records with timestamps equal to the boundary. It does not stop at the first
later timestamp because record timestamps need not be monotonic.

Use a query such as:

```text
value.orderId == "123" AND headers.source == "checkout"
value.total >= 100 AND key contains "order-"
offset > "9007199254740992"
value == null
```

Conditions are joined with `AND`. Operators are `==`, `!=`, `contains`, `>`, `>=`, `<`, and `<=`.
Values are JSON literals. JSON value fields use dot paths; header names follow `headers.`.
Offsets and timestamps require quoted non-negative decimal strings to preserve integer precision.
Numeric comparisons apply to JSON numbers and partition numbers. Missing fields do not match,
including `!=`; malformed JSON cannot match nested JSON fields. Queries are interpreted,
never evaluated as JavaScript. They currently inspect UTF-8/JSON payloads; schema-framed binary
records can be decoded separately in **Schemas**.

Searches can be saved in this browser's local storage, scoped to the active profile and Studio's
origin. Exported results include bytes and coverage. Do not include credentials in query values.

**Correlate** matches an exact field value across the selected topics and sorts by record timestamp.
Choose **Text** for keys, headers, or string IDs; choose **JSON literal** for numeric IDs, booleans, or null.
These matches indicate correlation, not proven causality or application processing latency.

### Limits and coverage

A scan is bounded to 30 seconds, 16 MiB of examined record data, at most 10,000 examined
records, and 1,000 returned records across all topics. The default examined-record limit is
1,000. At most four scans, replays, or scenarios run concurrently per server.

Expand **Scan coverage** to see each partition's start, captured end, next offset, and completion.
Reaching a result, record, byte, or time limit is reported as partial. No matches in a partial scan
does not establish that no matching event exists. **Cancel operation** stops reading; it does not
return a final scan result. Network and broker connection setup can still take the client's
configured connection timeout to unwind.

## Replay and dead-letter recovery

Select up to 100 search results and choose **Stage selected for replay**. In **Replay**, choose a
destination topic, inspect or edit the record preview, and choose a send rate of 1–100 records/s
(default 10). The final replay button sends the previewed records.

The JSON preview represents keys, values, and headers as base64, preserving arbitrary bytes.
`null` values remain tombstones. Repeated header values retain their order, including null entries.
Original partitions, offsets, and timestamps are not copied: the destination producer assigns
partitions and the broker assigns new offsets. Records are acknowledged individually and results
show each send's success or failure. Failed delivery can be uncertain; inspect the destination
before retrying. Replaying can repeat application side effects. Cancelling cannot undo acknowledged
or already in-flight writes.

## Inspect compacted state and partitions

**State** shows the latest observed value for each topic/partition/key and expandable observed
history. Tombstones are marked as deleted, not hidden. Null keys are excluded. A time-filtered or
partial scan is a partial state view. Key histories remain partition-local because partition-count
changes or custom routing may place the same key in multiple partitions. Even a full scan cannot
recover versions already removed by retention or compaction.

**Partitions** predicts a UTF-8 key's partition using core's Default or Legacy partitioner and the
first selected topic's current metadata. Custom and sticky routing are not predicted. Scan results
also show sample counts per topic/partition and frequently observed keys; these counts describe
the bounded sample, not whole-cluster traffic rates.

## Schema Registry

Configure a Confluent-compatible registry on the server, separately for each profile:

```js
export default {
  client: { brokers: ['localhost:9092'] },
  studio: {
    schemaRegistries: {
      default: { url: 'http://localhost:8081' },
      staging: {
        url: 'https://registry.example.com',
        username: process.env.REGISTRY_USERNAME,
        password: process.env.REGISTRY_PASSWORD,
      },
    },
  },
};
```

`default` configures the unnamed connection. Named entries match `cli.profiles` names. As an
alternative for the unnamed connection only, set `KAFKA_STUDIO_SCHEMA_REGISTRY_URL`,
`KAFKA_STUDIO_SCHEMA_REGISTRY_USERNAME`, and `KAFKA_STUDIO_SCHEMA_REGISTRY_PASSWORD`.
The browser cannot supply registry URLs or credentials. Registry configuration never appears
in the profiles response. URLs must use HTTP(S), without embedded credentials, query, or fragment.

**Schemas** lists subjects and versions and loads a schema's ID and definition. **Validate and
encode** accepts a JSON payload and returns Confluent-framed base64 bytes for Avro, JSON Schema,
or Protobuf. For Protobuf, an optional fully qualified message type chooses a non-first or nested
message. Without it, the first declared message is used. Protobuf payloads accept decimal strings for 64-bit integers, enum names, and base64 byte fields,
including in nested, repeated, and map fields. Values outside their signed/unsigned range, unsafe
JSON integer numbers, invalid primitive types, and unknown fields are rejected. Payload validation
is not a schema compatibility check. Decode handles
Confluent message indexes. Schema references are resolved from the configured registry; Protobuf
resolution is bounded to 32 references and 16 levels of message nesting.

Use **Decode with schema** on a search result or paste base64 into **Decode payload**. Encoded
bytes can be staged for replay, inspected, and sent. Studio never registers schemas or changes
registry configuration. Payload validation does not assert compatibility with other schema versions.

## Consumer lag diagnostics

Open a consumer group to collect samples every five seconds while its page is open. Studio
retains 60 samples in memory and shows known lag, uncommitted partitions, incoming offset rate,
committed offset rate, and catch-up estimates. Commit movement can include offset skips and is
not proof of processing. Estimates assume current rates continue; they are absent when net
commit progress is non-positive, commits are missing, or offsets move backwards. A partition
with lag and no observed commit progress for at least 30 seconds is marked as stalled. Session
history is not persisted and sampling pauses when the page closes or browser polling pauses.

## Repeatable scenarios

In **Scenarios**, edit or import a version-1 JSON scenario. Steps produce messages or wait for
matching events. `{{id}}` is replaced with a fresh ID throughout each run's keys, values, headers,
and predicate values. Choose predicates that include that ID to avoid unrelated-event matches.
Expectation topics' end offsets are captured before the first produce step so fast replies can
still be observed. Each expectation has a 100–30,000 ms timeout, default 10,000 ms. Runs contain
at most 20 steps and have a two-minute overall deadline. The first failed step stops the run.

```json
{
  "version": 1,
  "name": "Order flow",
  "steps": [
    { "kind": "produce", "topic": "orders", "key": "{{id}}", "value": "{\"id\":\"{{id}}\"}" },
    {
      "kind": "expect",
      "topic": "payments",
      "predicates": [{ "field": "value.id", "operator": "eq", "value": "{{id}}" }],
      "timeoutMs": 10000
    }
  ]
}
```

Export the file and run it through Studio's standalone binary:

```sh
kafka-studio --scenario kafka-scenario.json
kafka-studio --scenario kafka-scenario.json --profile staging
```

The runner uses the same config discovery and environment precedence as Studio and opens no
browser or HTTP server. It prints JSON results and exits `0` on success, `1` on failure,
`2` for invalid flags, or the signal exit code on interruption. Files are capped at 1 MB.
`--profile` requires `--scenario`; scenarios refuse both `--read-only` and `studio.readOnly`.
The `kafka studio` wrapper and embedded terminal do not accept these headless flags.

## Compare environments

**Compare** reads topic configuration, partition count, and per-partition replica counts from the
active profile and another configured profile. It does not switch the active connection or mutate
either cluster. Sensitive config values are excluded. It also reads each registry's latest
`<topic>-value` schema, reporting unavailable registries separately. Inspect schema definitions
and versions, not IDs alone: registry IDs are local to an environment. Snapshots include declared
references and up to 32 resolved referenced definitions within a 30-second deadline; failures or
limits report the schema as unavailable rather than implying a complete comparison. Topic or permission errors
are reported instead of being presented as empty differences. The comparison does not apply
changes or perform a schema compatibility check.
