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

The **Console** page runs read-only CLI-style commands against the active connection, such as
`topic list`, `topic describe orders`, `topic offsets orders`, `group offsets payments-worker`,
and `config describe --type topic orders`. Run `help` for the supported commands (including cluster,
ACL, quota, and transaction inspection), or append `--help` for usage. A leading `kafka` is optional.

Suggestions include commands, flags, allowed values, and live topic/group names. **Tab** accepts
one; **↑ / ↓** selects suggestions. **Escape** dismisses suggestions, after which **↑ / ↓** recalls
command history. **Enter** runs. Use **Escape**, then **Tab**, or **Shift+Tab** to leave the editor.
Quote group names containing spaces, for example `group describe "payments worker"`. Use `--`
before names beginning with `--`, for example `group describe -- --reader`; autocomplete inserts
this marker when needed. Double-quoted JSON escapes preserve tabs and other control characters.

The terminal shows commands in execution order with collapsible output beneath each command and
a fixed prompt at the bottom. Suggestions overlay the transcript without moving the layout.
`help` prints a readable command reference; Kafka results are JSON. Copy individual output or
the whole transcript, and clear output with the header button or **Ctrl+L** from the prompt.
Each command shows its duration and success/error status. Scrolling up pauses automatic following;
returning to the bottom or running another command resumes it. The page keeps up to
20 results and 50 unique history entries in memory; navigating away or switching profiles clears
both. The console supports the listed inspection commands, including in `--read-only` mode.
Shell execution, file arguments, connection overrides, arbitrary `admin call`, and mutations are
unavailable.

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
