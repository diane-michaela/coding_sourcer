# Agent Pierre-Richard DUPONT (PRD)

A Claude Code skill. Invoke with `/PRD` in any project, anywhere on this machine — it's
installed globally via a symlink at `~/.claude/skills/PRD`.

## What he does

Pierre-Richard DUPONT (P.R.D.) interviews you before a prompt goes out — a research brief, a
PRD ask, a request to turn a PRD into a build plan, or any other prompt you want a second pass
on. He doesn't write the deliverable. He sharpens the ask: catching vague specs, stated-vs-real
problem gaps, and unverifiable acceptance criteria before they cost you a wasted research pass
or a build that misses the point.

## How to use it

```
/PRD
[paste your draft prompt / brief / PRD ask]
```

He'll classify what kind of ask it is, run the matching interview (a few questions at a time,
never a dump), self-check for gaps, then hand back a tightened version and ask you to confirm
before it goes anywhere.

## Where this came from

The question banks in `SKILL.md` are condensed from a four-stage prompt chain documented at
`wiki/templates/prd-agent.md` in the RecOps Obsidian vault:

1. Deep research briefing (what to look into + why, Research First)
2. Deep research meta-prompt (the full Q&A → summary → research → findings process)
3. PRD build via plan mode ("Marlowe") — don't-echo guardrail, quality standards first,
   stranger test
4. PRD → implementation plan interview — research/evaluation/dry-run/verification

That page also carries the external grounding (GitHub Spec Kit, Thoughtworks, Addy Osmani,
Reforge/Builder.io) behind the Given/When/Then and stranger-test guardrails used in Lane 2
here. This skill is the operational, always-available version of that same discipline — the
Obsidian page is the fuller writeup and source of truth if the two ever drift.

## Files

- `SKILL.md` — the actual skill definition (frontmatter + operating instructions Claude Code
  reads when `/PRD` fires)
- `README.md` — this file
