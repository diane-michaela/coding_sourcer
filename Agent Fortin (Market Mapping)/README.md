# Fortin Market Mapping — Cartographier un marché

A Claude Code skill for mapping a talent market (key employers, their ecosystem, and how people
move between them) **before** candidate-level sourcing starts. Named after **Pierre-André Fortin**,
founder of **Anara** (Paris headhunting cabinet, est. 2018), whose published method is the direct
source.

`.claude/skills/Fortin Market Mapping` in the workspace root is a symlink into this folder, same
pattern as `Agent Bliard`, `Irina LinkedIn Lite`/`Irina LinkedIn Recruiter`, and `Vlastelica
Intake Prep`.

## Why this exists, and what it doesn't replace

Irina LinkedIn Lite/Recruiter and Agent Bliard both assume you already know who/what you're
searching for — they turn a target profile into a boolean or X-ray string. Market mapping is the
layer *before* that: figuring out which companies even have the people you want, and how talent
moves between them, so the sourcing plan starts from evidence instead of a guessed competitor list.
This skill produces a company list and a market/flow read; it hands off to Irina or Agent Bliard
once it's time to build an actual search query, rather than duplicating that.

## How this started

Diane asked a general question first — who works on "talent mapping" in French and English,
naming Pierre André, "director of Hamara," as someone who'd written on the subject. That specific
attribution didn't resolve: no findable person or firm called "Hamara" tied to talent mapping
turned up across several search variants (French and English, different spellings). Diane then
supplied the actual URL — **anara.fr** — which resolved it: the real name is **Pierre-André
Fortin**, founder of **Anara**, not "Pierre André" of "Hamara." Likely a mishearing/misremembering
of the firm's name. Worth remembering this founder/firm pairing correctly going forward.

## Research process and source quality

**Pass 1 — broad landscape (English + French podcasts, articles, people).** Turned up the general
talent-mapping/market-mapping content ecosystem: Matt Alder (*Recruiting Future*), Johnny Campbell
(SocialTalent, *Hiring Excellence*), Glen Cathey/Irina Shamaeva/Shally Steckerl (sourcing-community
names already covered by the Agent Bliard skill), and French podcasts (*Entre recruteurs*, *Le
Meilleur du Recrutement*, *Tam Tam*, *Le Barbu qui parle RH*, *Dear Talent*). None of these turned
out to be primary methodology sources for market mapping specifically — mostly general recruiting/
sourcing content that touches the topic in passing.

**Pass 2 — resolving Anara and pulling the actual methodology.** Once anara.fr was confirmed as the
real source, fetched Fortin's own article
([Market Mapping : Révéler les Talents Invisibles](https://anara.fr/market-mapping-reveler-les-talents-invisibles/))
and training page ([Formation Market Mapping](https://anara.fr/formation-market-mapping/)) directly
— this is genuine first-party teaching content (the 4-phase method, the "Fifth Element" technique,
the Veeva/pharma case study with real before/after numbers), not a bio or marketing page. This is
now the skill's backbone.

**Pass 3 — cross-referencing English-language material for a fuller picture.**

- **SocialTalent glossary** ([Market Mapping](https://www.socialtalent.com/glossary/market-mapping),
  [Talent Mapping](https://www.socialtalent.com/glossary/talent-mapping)) — the clearest available
  statement of the market-vs-talent-mapping distinction (aggregate picture vs. named-candidate
  list), including the four-dimension framework (supply/demand/compensation/competitive dynamics).
  Credible: part of a paid, established sourcing-training curriculum (Licensed Master Sourcer), not
  a marketing funnel.
- **Toby Culshaw**, book *Talent Intelligence* (Kogan Page, 2022) + *Talent Intelligence Collective
  Podcast* — the closest English-language canonical text on the discipline as a whole. Broader
  scope than sourcing alone (workforce planning, business framing) — used here for the
  "decision-ready deliverable" framing in Phase E, not for tactical technique.
- **Stratigens** (Alison Ettridge, acquired by Lightcast 2024) — a genuine labor-market-data vendor
  in this space, cited for the concept, not mined for proprietary technique (their actual method is
  behind a paid data platform).
- **SourceCon archive** — practitioner-community talks on talent mapping (Natalya Kazim among
  contributors); corroborates that this is an established sourcing-community topic, not just an
  Anara-specific framing.
- **Intellerati / The Good Search** (executive search research lab) — the older retained-search
  "name generation" tradition (org-chart building, verified via press releases/filings, inferred
  from title levels). Genuinely different lineage from the sourcing-community material above —
  included as an alternate Cartographie technique for when target companies are already named,
  rather than folded into Fortin's method as if it were the same thing.

**Flagged as low original value, not used as a source:** a cluster of near-identical "Market
Mapping 101 / 5 steps" SEO articles (QX Global Group, MightyRecruiter, Floodgate Medical, Venn,
Beeskneeshire, Loxo, Recruiterflow, Multirecruit) that repeat the same generic step list almost
verbatim across sites — the same pattern the Agent Bliard research flagged in HR-SaaS glossary
boilerplate (Asanify/Taggd/Qandle). Useful only as confirmation that "market mapping" is a
widely-recognized term, not as methodology. Also flagged: the YouTube video *"Market Mapping
Secrets TOP Recruiters Use to Find Hidden Talent"* is sponsored content for a market-mapping SaaS
tool (MarketMapr/RecMapper) — watchable for technique ideas, treated as vendor marketing rather
than methodology authority, same caveat the Agent Bliard research applied to Pin.com/Lessie.ai.

## Files

- `SKILL.md` — the skill definition Claude Code reads when this fires.
- `references/market-mapping-method.md` — Fortin's 4-phase method, the Veeva worked example, his
  training curriculum, SocialTalent's market-vs-talent-mapping distinction and four dimensions, the
  executive-search name-generation technique, realistic expectations, and a further-reading list.
- `README.md` — this file.

`.claude/skills/Fortin Market Mapping` in the workspace root is a symlink to this folder — that's
what Claude Code actually loads.
