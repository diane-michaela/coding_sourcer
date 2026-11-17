# Google boolean rules for LinkedIn job postings

These rules are for Google, searching LinkedIn job pages. They are not LinkedIn Recruiter rules.

## Operators

| Need | Write it like this | Why |
|---|---|---|
| Mandatory term | `bedrock langgraph` | Google treats a space as AND. The word AND is not an official Google operator. |
| Alternatives (short query) | `(langchain OR llamaindex)` | Readable while the query is short. |
| Alternatives (query over 35 characters) | `(langchain \| llamaindex \| crewai)` | Same meaning as OR, saves characters on long queries. |
| Exact phrase | `"semantic kernel"` | Keeps multi-word terms together. |
| Exact single word | `"js"` | Only when Google would swap it for a synonym (js becomes javascript). |
| Role focus | `(intitle:engineer \| intitle:ingénieur)` | Limits to postings whose title has the word. Repeat intitle: for each term. |
| Exclude | `-stage -alternance -internship -freelance` | Only when the user asks to exclude something. Never added by default. |
| Country | `site:fr.linkedin.com/jobs/view` | Country subdomain plus the job page path. |
| Freshness | add `&tbs=qdr:m` or `&tbs=qdr:w` to the URL | Google results do not show posting dates, this is the only way to favour recent ones. |

## Do not use

- `AND` as a word.
- `allintitle:`, `allinurl:`, `allintext:`, `AROUND()`, `cache:`. They no longer work.
- Quotes around every term. That is not stricter, it just blocks useful synonyms.
- LinkedIn Recruiter operators (`headline:`, `skills:`, `seniority:`, `yoe:`, `functions:`, `companysize:`).

## Limits

- About 32 words per query. Google ignores the rest without warning. If results look too broad, count words before blaming the logic.
- 10 results per page, roughly 100 to 120 results per query at most before the "omitted entries" notice. For bigger markets, split the query (one per framework, or one per city) and merge on job ID.
- The country subdomain is a good proxy for job postings but not perfect: some rows come back with odd locations. Flag them, do not drop them silently.

## From a job description to a query

1. List the must-haves. Each becomes a space-separated term.
2. Group alternatives for the same need (several frameworks, several cloud providers) into one `|` group.
3. Leave nice-to-haves out of the query. Keep them as a scoring list for Phase D.
4. Decide stack focus or role focus. Add intitle: only for role focus.
5. Add the country site: term first. Add exclusions last, and only if the user asked for them.

Example JD: "Python and Kubernetes required, Terraform or Pulumi, Datadog a plus, platform engineer role, France"

- Stack focus: `site:fr.linkedin.com/jobs/view python kubernetes (terraform | pulumi)`
- Role focus: `site:fr.linkedin.com/jobs/view python kubernetes (terraform | pulumi) (intitle:platform | intitle:devops | intitle:sre)`
- Scoring list for later: Datadog.

## Other countries

Replace the subdomain: uk.linkedin.com, de.linkedin.com, es.linkedin.com, it.linkedin.com, nl.linkedin.com, be.linkedin.com, ch.linkedin.com. Check a first page of results before trusting a new subdomain.
