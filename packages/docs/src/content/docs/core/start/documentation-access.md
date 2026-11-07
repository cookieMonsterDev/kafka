---
title: Documentation for tools and agents
description: Read Markdown, machine indexes, versioned sources, and package agent skills
order: 5
section: start
---

Every page has **View Markdown** and **Copy page** controls. Markdown is generated from
the same source as HTML, with absolute links, fenced examples, package version, canonical URL,
and source revision. It contains article content rather than menus and search controls.

## Discovery

- [Agent index](../../../../llms.txt) — a Markdown link index grouped by package.
- [Documentation manifest](../../../../docs-index.json) — schema version, package versions,
  source revision, canonical URLs, Markdown URLs, headings, and published skills.
- [Sitemap](../../../../sitemap.xml) — canonical HTML pages, including individual CLI commands.

Each HTML page advertises its Markdown representation with `rel="alternate"` and the agent
index with `rel="describedby"`. These exports help tools retrieve relevant content; they do not
guarantee discovery or search ranking. Essential content and package links work without JavaScript.

## Match your installed version

The page toolbar identifies the documented package version. Packages release independently.
Prefer the [skills bundled with your installed package](../agent-skills/) when using an older release.
The manifest identifies the exact source revision of this build. Local uncommitted edits are not
part of that revision; publication builds use a clean CI checkout.

For historical content, open the [docs release list](https://github.com/cookieMonsterDev/kafka/releases?q=docs-v&expanded=true),
choose a `docs-v…` tag, and download its source archive. That immutable source snapshot contains
`packages/docs/src/content/docs/`, package manifests, and `packages/*/skills/`. Read manifests
from that snapshot to identify its package versions; a docs version is not a Core version.
The live website tracks the latest deployed release and does not claim to serve old HTML versions.

## Agent skills

[Agent skills](../agent-skills/) routes tasks to setup, producer, consumer, admin, migration,
CLI, Config, and Studio guidance. Website copies include their reference files; npm copies
remain the right choice for a version-pinned application.

## Crawlers and hosting

This project is hosted under `/kafka/`. A robots policy is effective at the origin-root
`/robots.txt`, which is managed by the owner of `cookiemonsterdev.github.io`, not this project's
`/kafka/` directory. The project advertises its sitemap and ordinary links instead of shipping
an ineffective project-level robots policy. Crawlers should respect the origin's policy.
