---
title: Output and scripting with JSON
description: Machine-readable output, exit codes, and jq recipes
order: 2
section: guides
---

Human output goes to stdout as a table or a short line, meant to be read, not parsed. Add `--json`
(or `--format json`, or set `KAFKA_OUTPUT=json`) and every command writes exactly **one** JSON
document to stdout instead — the shape a script should parse.

```sh
kafka topic list --json
kafka topic describe orders --json | jq '.'
```

## Why not a number

A `bigint` (an offset, a timestamp in some responses) becomes a **decimal string**, not a JSON
number: `JSON.stringify` throws on a bare `bigint`, and a plain `number` starts losing precision
past 2^53 — silently wrong offsets are worse than a string that needs one `Number()`/`BigInt()`
call on the way out.

```sh
kafka topic offsets orders --json | jq -r '.partitions[].offset | tonumber'
```

## Binary fields

A `Buffer` (an ACL host pattern's raw bytes, SCRAM material, a delegation token HMAC) becomes a
**base64 string**. The one exception: a 16-byte `Buffer` under a `topicId` key becomes its UUID
string instead, matching how every other Kafka tool already prints a topic id.

## Quiet, verbose, and color

`-q`/`--quiet` silences everything but errors; `-v`/`--verbose` (repeatable, `-vv`) adds detail —
all of it on stderr, never mixed into the one JSON document on stdout. `--no-color`/`NO_COLOR`
disable color; `FORCE_COLOR` forces it even off a TTY. None of these affect `--json`'s output shape.

## Exit codes

Check the exit code before parsing stdout — a non-zero code means the JSON document (when present
at all) describes a failure, not a result. See [Exit codes](../reference/exit-codes/) for the full
table; `4` (partial batch failure) is the one worth calling out here, since it's specific to this
CLI's fan-out commands: passing multiple topics/principals/ids to a command that issues one call
per item exits `4` when some succeeded and others didn't, with each item's own outcome in the JSON
body.

```sh
kafka topic create orders payments --json > result.json
code=$?
if [ "$code" -ne 0 ] && [ "$code" -ne 4 ]; then
  echo "failed outright" >&2
  exit "$code"
fi
jq '.results[] | select(.ok == false)' result.json
```
