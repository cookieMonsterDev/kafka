---
title: API reference
description: The studio server's HTTP surface, its response envelope, and its security model
order: 1
section: reference
---

The studio's browser UI talks to its own `node:http` server over plain REST, plus Server-Sent
Events for the three live streams (message tail, burst progress, the activity firehose). Every
response is `application/json`, with `bigint` offsets serialized as decimal strings. Errors use real
HTTP status codes and one envelope:

```json
{ "error": { "code": "unknown_topic", "message": "topic \"orders\" not found" } }
```

A mutating route additionally returns `403` with `{ "error": { "code": "read_only", ... } }` when
the server was started with `--read-only`.

## Routes

| Method   | Path                                            | What it does                                                 |
| -------- | ----------------------------------------------- | ------------------------------------------------------------ |
| `GET`    | `/api/health`                                   | Version, read-only flag, host/port                           |
| `GET`    | `/api/profiles`                                 | Configured `cli.profiles`, credentials redacted              |
| `POST`   | `/api/profiles/active`                          | Switch the active connection profile                         |
| `GET`    | `/api/cluster`                                  | Connection status                                            |
| `GET`    | `/api/topics`                                   | List topics with partition/replication counts                |
| `POST`   | `/api/topics`                                   | Create a topic                                               |
| `GET`    | `/api/topics/:name`                             | Partitions, ISR, offsets, size, configs                      |
| `PATCH`  | `/api/topics/:name/configs`                     | Set/unset topic configs                                      |
| `POST`   | `/api/topics/:name/partitions`                  | Raise the partition count                                    |
| `DELETE` | `/api/topics/:name`                             | Delete a topic                                               |
| `POST`   | `/api/topics/:name/records/delete`              | Delete records before a given offset per partition           |
| `POST`   | `/api/topics/:name/offsets/by-time`             | Resolve offsets at or after a timestamp (read-only)          |
| `GET`    | `/api/topics/:name/messages`                    | A bounded page read (seek + limit)                           |
| `GET`    | `/api/topics/:name/tail`                        | **SSE** — live messages as they arrive                       |
| `POST`   | `/api/produce`                                  | Send one or more messages, waiting for acknowledgment        |
| `POST`   | `/api/produce/burst`                            | Start a rate-limited burst; returns a `jobId`                |
| `GET`    | `/api/produce/burst/:jobId`                     | **SSE** — burst progress                                     |
| `DELETE` | `/api/produce/burst/:jobId`                     | Cancel a running burst                                       |
| `GET`    | `/api/groups`, `/api/share-groups`              | List consumer groups / share groups (KIP-932)                |
| `GET`    | `/api/groups/:id`                               | Members and per-partition lag                                |
| `POST`   | `/api/groups/:id/offsets/reset`                 | Reset committed offsets (earliest/latest/timestamp/explicit) |
| `DELETE` | `/api/groups/:id/offsets`                       | Delete committed offsets for given topic partitions          |
| `POST`   | `/api/groups/:id/members/remove`                | Remove members from a group                                  |
| `DELETE` | `/api/groups/:id`                               | Delete a group                                               |
| `GET`    | `/api/share-groups/:id`                         | Share group members and offsets (read-only)                  |
| `GET`    | `/api/events`                                   | **SSE** — instrumentation firehose behind the flow board     |
| `GET`    | `/api/acls`, `/api/quotas`, `/api/transactions` | Read-only cluster inspection                                 |

Message keys/values cross the wire as base64 (they're arbitrary bytes, not necessarily UTF-8); the
web UI decodes them per the viewer's chosen decoder. Sending a message accepts plain strings for
`key`/`value` directly — `value: null` is Kafka's tombstone, an explicit delete marker rather than
"no value".

## Security

A local server that can speak to a possibly-production Kafka cluster is a real attack surface, so
none of this is optional:

- **Localhost by default.** The server binds `127.0.0.1` unless told otherwise with `--host`, which
  prints a loud warning when used.
- **A session token per process.** Delivered once, in the URL's hash (`#token=…`, never a query
  string or header the server itself would log), and required on every `/api/*` request afterward
  as `x-kafka-studio-token` (or, for the SSE routes, a `token` query parameter).
- **An Origin/Host allowlist**, rejecting a request naming a `Host` (or `Origin`) other than the
  address the server was told to bind — the standard defense against DNS rebinding against a local
  dev server.
- **`--read-only`, enforced server-side.** A mutating request is rejected with `403` regardless of
  what the client sent, not merely hidden behind disabled buttons in the UI.

Next: [Running it against a broker](../../guides/local-development/).
