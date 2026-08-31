# X (Twitter) Presence Strategy — Diane Rocher, Technical Talent Partner

## Where things stand

You have a feed full of posts and about 10 followers. That combination almost always points to the same root cause: the content is fine, but there is no distribution loop feeding it. On X, posting alone does not surface you to anyone outside the handful of people who already follow you, and with 10 followers that handful is essentially empty. Growth on this platform comes overwhelmingly from being seen by other people's audiences first — through replies, quote tweets, and being engaged with by accounts that already have reach — and only secondarily from your own posts. Fixing distribution, not content quality, is the highest-leverage move here.

## The mental model for the next 90 days

Think of X growth as two separate engines that need to run at the same time: a content engine (what you publish on your own profile) and a distribution engine (how you get put in front of people who don't follow you yet). Most people who "post good stuff and don't grow" are running the content engine alone. Given that your content is already good — hiring and recruiting insight is a genre with real appetite on X right now, especially with the tech hiring market as active as it is — the fix is almost entirely on the distribution side.

## Content: keep it focused, make it more shareable

Hiring and recruiting insight is your strongest lane and you should stay narrow rather than broaden it. Within that lane, the formats that travel best on X are specific and slightly contrarian observations about the hiring process (what a bad take on interviewing gets wrong, what candidates misunderstand about comp negotiation, what a JD reveals about a company's actual culture), short breakdowns of a real hiring decision you made and why, and threads that teach one concrete skill (how to read a technical resume, how to structure a scorecard, how to negotiate an offer as a candidate). Numbers and specificity outperform generic advice — "I rejected 40 resumes this week, here's the one line that killed 30 of them" will always outperform "tips for writing a better resume." Posting three to five times a week, with one heavier piece (a thread or a longer take) and the rest short single-idea posts, is a sustainable cadence that won't burn you out while you build the audience that makes higher cadence worthwhile later.

## Distribution: where the actual growth happens

Before your own posts can travel, you need to be visible in other people's replies. The single highest-ROI habit for someone starting from near-zero followers is spending 20-30 minutes a day finding 10-15 posts from accounts your target audience already follows (bigger voices in tech recruiting, engineering leadership, and tech hiring commentary) and leaving a genuinely useful reply — not "great post!" but a reply that adds a fact, a counterpoint, or a short version of your own experience. Done consistently, this is what gets your name in front of audiences 100-1000x the size of your own, and it's also exactly the kind of activity Phantombuster can help you scale the targeting of, even though the reply itself should stay human-written (more on why below).

## What Phantombuster can safely automate here — and what it can't

Phantombuster's current X/Twitter toolkit is built around a small set of phantoms: an Auto Liker, an Auto Follow, an Auto Retweeter, an Auto Poster (scheduled posting from a spreadsheet), a Tweet Extractor, and a Tweet Likers Export. There is no safe automated commenting tool, and that's not an oversight — X now actively flags automated reply behavior (identical templated replies, reply speed faster than human typing, reply bots on high-profile accounts) as a suspension trigger, and Phantombuster does not offer an X auto-commenter for that reason. The two things worth automating with what you have are finding and organizing the right accounts to engage with, and light-touch warm-up engagement (likes) on those accounts so your profile starts showing up in their notifications before you ever reply.

Concretely: you already have a Twitter/X Profile Scraper and a Twitter/X Profile URL Finder in your account, currently set up for lead enrichment. The same tools can build you a target list — for example, scraping the followers or engaged commenters of 5-10 well-known tech-recruiting or engineering-leadership accounts to assemble a list of 100-300 accounts worth engaging with regularly. An Auto Liker run against that list, spread across the day at a conservative volume, keeps you visible to them passively. Comments and replies stay manual and go on top of that list — you're not writing from scratch, you're working through a pre-built queue of the right people.

On limits: Phantombuster's own guidance for the Auto Liker caps out at 1,000 likes/day, but that ceiling is for mature accounts running at volume — starting out, staying in the range of 30-80 likes/day spread across 3-4 launches is far safer and matches your actual reply capacity anyway (there's no point liking 500 accounts a day if you can only meaningfully reply to 15). The same "spread it out, don't automate a single burst" logic applies to Auto Follow if you use it — and current guidance across the platform is to stay well under any stated limit rather than push right up against it, since X's detection increasingly flags activity that's too regular or too fast rather than just too high in volume.

## A realistic weekly routine

Monday through Friday: 20-30 minutes reviewing the target-account list (refreshed periodically via the scraper) and manually replying to 8-12 recent posts from those accounts. Three to five original posts a week from the content lane above, timed to when your target audience is active rather than batch-posted. A low-volume Auto Liker run (30-50 likes) against the target list once or twice a day, timed a few hours apart rather than back-to-back. No automated replies, retweets used sparingly and only on posts you'd genuinely want associated with your name, since retweet volume is also watched.

## Risk boundaries worth keeping in mind

X suspends accounts for patterns, not for any single action: identical templated content at scale, engagement speed faster than a human, multiple tools running simultaneously, and reply/DM spam are the clearest triggers. Sticking to likes and follows via Phantombuster, keeping volumes conservative (especially in the first 4-6 weeks on a smaller account), and keeping every comment and reply human-written protects you from all of the current suspension triggers while still giving you the leverage of not having to manually hunt for who to engage with every day.

## Build log (2026-08-13)

What's live in the Phantombuster account so far, all seeded from @HungLee (Curator, Recruiting Brainfood — 13.9K followers, strong RecOps/TA-ops fit):

- **Paused** the old `[enrichment] Twitter/X - Profile URL Finder`, which was running on a silent daily 8am schedule — switched to manual-only so it can't run concurrently with the new phantoms and read as "multiple tools running simultaneously" to X.
- **Created** `[growth] Twitter Follower Collector` — pulls @HungLee's ~70 most recent followers (X's hard platform cap; can't be raised, even across repeat runs).
- **Created** `[growth] Twitter Following Collector` — pulls the accounts @HungLee follows (up to ~3,019, no stated cap found — a curated signal, arguably more valuable than his followers since he chose these accounts).
- **Created and launched (one-time)** `[growth] Hung Lee Auto Liker` — liked his 3 most recent tweets as a first warm-up touch. Not scheduled to repeat.
- **Tested and ruled out** `[growth] Twitter Tweet Likers Export` against one of Hung Lee's actual tweets — came back with only 2 likers despite the tweet showing meaningfully more likes than that on X itself. Same pattern as the Follower Collector's 70-follower cap: X severely truncates "who liked this" for tweets that aren't your own. Not a usable source for finding his audience — dropped from the plan. (The Following Collector's near-complete 2,983/3,019 pull remains the strongest source we have.)

Reused the existing Twitter session cookie already connected via the browser extension rather than asking for it again.

## Build log (2026-08-14, reconstructed)

Not written up at the time, but confirmed from PhantomBuster/Drive state on 2026-08-17: the 544-account follower+following list was merged and deduped (0 duplicate handles, Hung Lee himself excluded), landed in a Google Sheet (`Hung Lee - X Target List (544)`), and `[growth] Hung Lee List Auto Follow` was created and launched against it at 15/day.

## Build log (2026-08-17)

- **Re-uploaded the same 544-row list** as a fresh Google Sheet (`hunglee-target-list-544`, full 8-column version with bio/certified/source) — functionally identical to the 2026-08-14 sheet the live Auto Follow phantom already reads from, just a more complete copy. Known cosmetic issue: Google's CSV importer mangled emoji (not other scripts — Persian/Cyrillic imported fine) in ~20% of bios; left as-is since PhantomBuster only reads `profileUrl`.
- **Found `[growth] Hung Lee List Auto Follow` already live** (created 2026-08-14, not previously logged here) — 4 runs completed, ~60 accounts followed so far, working sequentially through the list with no errors.
- **Changed its schedule** from one 15-follow burst/day at 9:30am to three 5-follow launches/day (9:30 / 13:30 / 17:30 Europe/Paris) — same 15/day total, but spread out per the risk-boundaries guidance above (avoid single-burst automation patterns).
- **Sequencing decided**: exhaust Hung Lee's 544-account list first (at 15/day, ~1 month) before adding the 4-7 additional seed accounts from step 4 below — one pool at a time rather than merging multiple sources in parallel.

## Next steps

1. ~~Review collector output~~ — done 2026-08-14.
2. ~~Merge + dedupe the two lists~~ — done 2026-08-14.
3. ~~Land the merged list in a Google Sheet~~ — done 2026-08-14 (and re-confirmed 2026-08-17).
4. **Add 4-7 more seed accounts** beyond Hung Lee to widen the pool past one person's network — deferred until Hung Lee's list is exhausted (see sequencing decision above). No reliable candidates found via generic web search for this specific niche (especially thin for the Make.com automation community). Best source is likely Diane's own feed/who she already follows, or a keyword-based X search (e.g. "RecOps", "TA ops", "sourcing", "Make.com automation") rather than a listicle.
5. ~~Decide on Auto Follow~~ — done 2026-08-14 (15/day), schedule tuned 2026-08-17 (3x5/day).
6. **Wire the weekly routine** (see above) around the finished list: Auto Follow now running unattended; still need Diane's manual 20-30 min reply block + 3-5 weekly original posts to actually happen on a cadence.

See the accompanying workflow diagram for how these steps connect end to end — note the diagram predates the 2026-08-14/17 updates above and may need a refresh.
