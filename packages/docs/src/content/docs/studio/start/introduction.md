---
title: Introduction
description: A local web UI for inspecting and driving an Apache Kafka cluster
order: 1
section: start
---

`@cookiemonsterdev/kafka-studio` is a local web UI for inspecting and interacting with an Apache
Kafka cluster — browse and manage topics, tail and produce messages, inspect consumer groups, and
watch a live view of traffic moving through the cluster. Launched from the command line and served
on `localhost`. It is built on
[`@cookiemonsterdev/kafka-core`](../../../core/start/introduction/) and
[`@cookiemonsterdev/kafka-config`](../../../config/start/introduction/) — no separate protocol
implementation, no separate config format.

**Status:** early. The CLI, HTTP server, and web shell run end to end: the studio can browse,
create, and configure topics; produce messages (single sends and rate-limited bursts); browse and
tail live messages; inspect consumer groups (members, per-partition lag, offset reset, deletion)
and share groups; render a live topology board of cluster activity; and browse read-only ACL,
client quota, and transaction state. Every session is authenticated, and `--read-only` is enforced
by the server, not just hidden in the UI — see [Security](../../reference/api/#security).

## Finding your way around

The cluster overview shows topic, partition, and broker counts, plus **Studio uptime** (the age
of the local server process, not the Kafka brokers). Quick actions lead to topics, the producer,
and consumer groups.

Open a topic and use **Browse messages** or **Produce** to carry that topic into the next screen.
Configuration has a key filter and an **Overrides only** view; changing these filters keeps pending
edits. Non-negative millisecond settings show a readable duration below the raw value. Negative
sentinel values stay unchanged because their meaning depends on the setting.

In Messages, **History** reads stored records and **Live** selects a stream of new records. Live is
a mode selector, not a pause button; switch to History to stop tailing.

## Debugging event flows

Open **Debugger** to search JSON fields and headers across topics, correlate events, replay
selected records, inspect compacted state and key distribution, use Schema Registry codecs,
run/export scenarios, and compare profiles. Group pages collect session lag diagnostics.
Read [Debug event flows](../../guides/debugger/) for coverage limits and setup.

## Terminal alongside the UI

Use **Terminal** in the top bar to open the Kafka shell below any page. Its header can expand it
across the workspace, restore the dock, minimize it, or end the session. Drag the top edge to
resize it, or focus the separator and use arrow keys. The shell keeps its session as you navigate
or minimize it. Switching profiles or ending the session starts a fresh shell. The sidebar's
**Terminal** entry and older `/console` links open the expanded shell.

The shell uses the real `kafka-cli` parser, help, output formatting and confirmation checks.
A leading `kafka` is optional. Try `help`, `topic list`, `topic create orders --partitions 3`,
`config describe --type topic orders`, or `group offsets payments-worker`. Add `--help` for
arguments or `--json` for JSON output. It supports CLI admin operations as well as Studio's
message helpers:

```sh
produce orders --key order-1 --value '{"id":1}'
consume orders --limit 10
consume orders --from earliest --partition 0
consume orders --tail
```

The prompt follows output in the terminal. **Tab** completes commands, flags and live resource
names; arrows edit the line or recall history. **Ctrl+C** interrupts a command or live tail.
**Ctrl+L** or `clear` clears the display, **Shift+Tab** leaves terminal focus, and `exit` ends the
session. Select text to copy it with your platform's copy shortcut. Destructive commands retain
CLI confirmation and force checks. Quote names containing spaces and use `--` before identifiers
beginning with `--`.

The active profile applies to both UI and shell. Server-enforced `--read-only` also covers shell
commands, including admin passthrough and producing messages. Host-file arguments, connection
overrides and local commands such as `init` are unavailable. This is a Kafka command shell;
operating-system commands and shell pipelines are not executed. See the [API reference](../../reference/api/#terminal-sessions)
for session limits and lifecycle.

If a reload or server restart leaves the tab without a valid token, Studio shows recovery steps.
Open the full token-bearing URL from the terminal that launched it in a new tab. The token stays in memory
only; retrying requests cannot restore it.

## Two ways to launch it

- **`kafka studio`** — a command in [`@cookiemonsterdev/kafka-cli`](../../../cli/start/introduction/),
  resolved at runtime: install the studio alongside the CLI and `kafka studio` launches it, with an
  install hint if the studio package isn't present.
- **`kafka-studio`** — the studio's own standalone binary, usable without the CLI at all.

Both start the same server; see [Installation](../installation/) for both paths, and
[Running it against a broker](../../guides/local-development/) for pointing it at a real cluster —
including a one-command Docker broker for trying it locally.

Next: [Installation](../installation/), [Running it against a broker](../../guides/local-development/),
[API reference](../../reference/api/).
