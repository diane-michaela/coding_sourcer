# The Reverse Search: How I Source ML Engineers by Mapping Companies Instead of Booleans

*A prequel to my [PhantomBuster MCP + Claude.ai piece](https://medium.com/phantombuster/phantombuster-mcp-claude-ai-how-i-built-an-ai-sourcing-machine-68140a41b39c) — this is how the target-company list in that story actually gets built.*

![Candidate-first search vs. company-first search](reverse-search-diagram.png)

---

Here's the why, before the how. Every recruiter hiring for AI roles right now is fishing in the same shrinking pond, running near-identical boolean strings against the same few thousand profiles that happen to say "LLM" or "RAG" on them. That pool isn't growing as fast as the number of teams hiring against it, so candidate-first sourcing increasingly means competing for the same handful of visible names everyone else already found. The bottleneck isn't who to reach out to. It's who to even look at in the first place. That's the problem company-first sourcing solves: it swaps a shrinking, self-reported keyword pool for a much better filter, the companies actually building this work right now, whether or not any one engineer inside them bothered to update their profile to say so.

For years, sourcing has meant one thing: write a good boolean string. Glen Cathey built a whole training discipline around it, the inclusive boolean: stack every job title and synonym with OR, wrap the must-have skills in AND, add an X-ray operator, and let the search engine surface anyone who matches. It's still one of the most useful skills a sourcer can have. I use it every week.

But when I opened our Machine Learning Engineer / AI Specialist req (a role built around agentic AI on AWS Bedrock, LangChain, RAG pipelines, fine-tuning) boolean alone wasn't cutting it. Here's what I did instead, and why.

### Why the boolean net has more holes than it used to

A boolean string makes two bets: that the person keeps their LinkedIn updated, and that they describe themselves the way you searched for them.

Fewer people do the first one. Someone who has spent the last year fine-tuning LLMs and building retrieval pipelines in production may still list "Data Scientist" up top, because that's what their title said the last time they bothered to edit the page.

The second bet keeps getting harder too, because titles move faster than the boolean strings written to catch them. Machine Learning Engineer, AI Engineer, Applied Scientist, AI Specialist, ML/AI Engineer: often the same job, described differently company to company, sometimes by the same company a year apart. Write the string tight and you cut out the person doing the work under a different title. Write it loose and you drown in noise. Add "RAG," "LangChain" or "Bedrock" as keywords and you only catch people who happened to type those exact words into their own profile. A lot of strong engineers never do.

### The flip: search the employer, not the person

So I searched for the company instead of the candidate.

The logic: if a company is currently hiring for a role that mentions agentic systems, RAG, LangChain, AWS Bedrock and MLOps in the same job post, that company is running exactly the kind of project I'm hiring for. Their engineers, whatever their own profile says today, are working on it right now. A job posting is a more current, more reliable signal than an individual's self-description: it's written specifically to attract that profile, it gets refreshed when the project changes, and it doesn't sit stale the way a personal page does.

I ran the search over LinkedIn Jobs instead of LinkedIn People, using the same keyword logic I'd normally boolean against a candidate:

```
("machine learning engineer" OR "AI specialist" OR "applied scientist")
AND ("LLM" OR "RAG" OR "LangChain" OR "AWS Bedrock" OR "fine-tuning" OR "agentic")
```

Instead of a shortlist of people, that produces a shortlist of employers: who is actually building this, right now, in production. Every company on that list is somewhere I can go source, X-ray, or simply keep an eye on, regardless of what any one profile happens to say.

I don't run this by hand. The search lives as a PhantomBuster agent, LinkedIn Search Export, pointed at a saved LinkedIn Jobs search URL instead of a people-search URL, which is the one setting that actually flips candidate-first into company-first. Watcher mode is switched on, so every run diffs its results against the last one and only keeps postings that are genuinely new, instead of re-surfacing the same 200 jobs every time. It's scheduled to fire on its own on a fixed day each week, and every hit gets logged straight to a CSV: company name, job title, location, nothing I have to retype or remember to go check. The only manual work left is swapping the keyword query when I open a new kind of role.

None of this needed code. Setting it up is the same motion as any LinkedIn Jobs search: type the keywords into the search bar, set the location, and copy the finished URL straight into the phantom as its input, alongside which LinkedIn seat it should log in as and how many results to pull per run. PhantomBuster owns the login session and the scraping, so the only real configuration is a search URL and a schedule. The CSV it drops every week is raw material, not a finished list. From there, I hand it to Claude through the PhantomBuster MCP connection to dedupe it against everything logged so far, rank the companies that keep reappearing, and surface which ones look worth a closer look, the second half of the pipeline I walk through in the piece this one leads into.

### Don't wait for the req. Map your competitors first

If you're sourcing for a product company built around a specific stack, don't run this "flux tendu," the just-in-time habit of only reacting once the request lands on your desk. If you already know your stack, or where it's heading, you already know roughly which competitors and adjacent companies are wrestling with the same problem. Map them before the req exists, not after.

This isn't the only sourcing method, and it doesn't replace boolean. Think of it as a way to refresh your read on who's genuinely building with the projects and keywords you care about, instead of relying only on how candidates choose to describe themselves.

### How to actually set this up

None of this needs an engineer. Here's the version I'd hand to another recruiter:

1. **Write the query like a boolean, but for a job post, not a person.** Take the skills and title variants you'd normally search a candidate for, and keep only what would realistically show up in a job description: required tools, stack, and title synonyms, joined with AND/OR the same way you already build a candidate string.
2. **Run it as a LinkedIn Jobs search first, by hand.** Paste the query into LinkedIn's own Jobs search, not People search, set your location filter, and check that what comes back actually looks like the roles you're picturing. Once it does, copy that URL. That URL is your real configuration.
3. **Point a scraper at the URL instead of typing it in every week.** In PhantomBuster, set up a LinkedIn Search Export agent, set its search type to "LinkedIn search URL," paste in the one you just built, and connect it to the LinkedIn seat it should run as. This is the only step that turns a one-off search into something that runs without you.
4. **Turn on watcher mode before anything else.** This is the setting that makes the whole thing worth automating: it diffs each run against the last one and only keeps postings that are genuinely new, instead of handing you the same 200 jobs every week.
5. **Pick a cadence and leave it alone.** Weekly is enough for most roles, monthly for a smaller or slower-moving niche. Resist checking it after every single run. The value here comes from letting a few weeks of results accumulate, not from reacting to the first one.
6. **Track it as one running list, not a folder of exports.** Every run adds new company names, job titles, and locations to a CSV. Append each week's new rows to a single running sheet or table rather than treating each export as its own file, and once a month, sort by company: one appearance might be a fluke, three appearances over two months is a team actively building what you're hiring for.
7. **Hand the running list to an LLM once it's too big to read by hand.** This is the step the follow-up piece covers: Claude, through the PhantomBuster MCP connection, dedupes the list, ranks companies by how often they reappear, and flags which ones are worth a closer look.

If you want to try it without committing to a new workflow, run it in parallel with a live req: build the Jobs-search version of a boolean you're already running for candidates, let both sit for a month, and compare what each one actually surfaced.

### One search is a snapshot. A routine is a database.

Run this once and you get a list of companies for today. Run the same keyword set weekly or monthly and it turns into something more durable: a growing record of which employers keep posting this exact combination, the ones consistently building the thing you're hiring for. New names surface, old ones drop off, and after a few months, what's left is a short, ranked list of companies worth targeting long before you have a role open for them.

That routine is exactly what feeds the company "waves" behind my PhantomBuster MCP + Claude sourcing setup. This is the part that happens before any candidate search starts: build the list of who's actually doing the work first, then go find the people inside it.
