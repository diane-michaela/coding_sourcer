# RecOps: Introducing PRD as an Agent

*I Followed My Own SDLC Advice for Months until it got messy. Now, shortcut at its extreme with a simple /PRD.*

![Pierre-Richard DUPONT, the /PRD interviewer agent](pierre-richard-dupont.png)

---

Happy Summer!

Are you still surviving from the heat wave of July and welcoming the new one of August? I did, but the courgettes from my garden didn't. I am living in Vendée, a place for frogs as we are supposed to get more days of rain than sunny day. Strange time we are living, isn't it?

So it s me, juggling between heat and AI. Working on my workflows, bringing more robustness. Feeling like Tony Stark somehow while the plants from my garden R.I.P.

A few months ago I wrote about how a senior engineer taught me to stop vibe-coding blind and start writing a PRD before prompting anything. Requirements first, code last. AI gives speed, structure gives reliability. I meant every word of it (Here is the full story: https://medium.com/@drocher/how-recruiters-can-use-sdlc-to-control-their-code-instead-of-hoping-ai-gets-it-right-5ab0609cf25d)

### The challenge

2 things:

First, here's the part a PRD template never covers on its own: it forces you to answer the right questions, but it says nothing about who's in the room asking them. Write one alone and you're the product manager, the stakeholder, and the reviewer all at once, nobody pushes back, so every blind spot you walked in with, you walk back out with too.

Second, even warned, not every sourcing script, every scraper, every Make scenario got a PRD first. Because, as a human, sometimes I forget, I just create a weird prompt sentence by laziness placing PRD inside, hoping the LLM will get it right. Everything was saved in my obsidian, but how many tokens has been sacrificed to find that little thing in that wave of data?

It s like with gym, you get one day a real gym coach that teaches you once all those hard core step, machines, right weight to lift, when, how long, ect, you try to save that info in some space of your head, rewriting the steps you have to do. Days go and you start to make mistake reproducing, and even if that state of art was saved somewhere, then come laziness and you end up doing 5% of the job, because being told is easy, creating a routine around it is an other level.

### Meet Pierre-Richard DUPONT

Initials on purpose, P.R.D (works with Pedro Ramon Delgadito too).

I was listening more and more about those agents from my company, but also private agents from personal contributors that were specialized in one part of the job. A PRD can become an agent then.

Pierre-Richard is a skill that now lives on my machine, and he does exactly one job: when I start to prompt a workflow, I run it through Pierre-Richard first. He doesn't write anything for me. He interviews me, the same interview every time, because now it's not up to my memory whether the whole thing shows up. It s simple to create it, fetch any repo that has all the knowledge pre-built or tailor it yourself and ask your LLM to create it in the best way that will suit you and your routine.

### What that actually looks like

Take something real: the intake meeting automation, the Make scenario that turns a recorded hiring-manager call into a Slack channel, a TA screening kit, and a JD, all on its own. One of its modules pulls the role name straight out of the recording's file name, so it can label the Slack channel. Simple enough, until you look at how it actually got built: a fixed character count. Chop the file name at a set position, call whatever's sitting there the role. It worked for exactly as long as every intake file kept the same shape. It didn't, and "replace the fragile filename-offset extraction with a proper delimiter split" is still sitting on my list of fixes to carry over to the next version.

That's what a prompt like "pull the role name out of the intake recording's file name" gets you when it goes straight to an agent: a solution built for the one example you had in front of you, and nothing else. Sound familiar?

Now imagine running that same ask through Pierre-Richard first. He reads it, decides it's PRD-shaped, and instead of a blank question, he proposes a guess: *"I'd assume the file name always follows the same fixed pattern. Is that guaranteed, or could the format change from meeting to meeting?"* Already worth stopping for, if I'm honest, I didn't know the answer myself back then. If it varies, he asks for the one thing that would have saved that module: *"(a) there's a consistent delimiter I can split on, (b) there isn't, we need a fallback, (c) something else."*

Etc, etc. Small question, whole rewrite avoided.

### Step-by-step implementation framework

You don't need Claude Code specifically for this, any tool that lets you save a standing prompt works the same way, a custom GPT, a Cursor rule, a Claude Project. But since people keep asking what's actually inside the folder (was a joke, they never ask), here's the real shape of it, no stand-in example.

#### Step 1: define the blueprint

Every small agent needs a strict box to play in. Here's how the four elements actually answer for mine.

**Role:** the system prompt, quoted plainly: "You are Pierre-Richard DUPONT (P.R.D., the initials are the point). Your one job: when invoked via /PRD, take whatever draft the user hands you and interview them until it's sharp enough to act on. You do not write the deliverable. You improve the ask."

**Inputs:** whatever the user actually hands it, a research brief, a PRD ask, a plan request, or a plain prompt. Nothing more. Not the whole conversation, not the whole codebase. It can look at project files already in view to make a better guess, never go looking for more on its own.

**Tools:** none. No file read, no file write, no terminal command. The entire mechanism is a conversation, ask, wait, adjust.

**Output:** not JSON, not a markdown template. It doesn't need to be machine-readable. It hands back a sharpened version of the ask in its own words and stops on an explicit confirmation question. A human reads it next, not a pipeline.

#### Step 2: choose your tech stack

No LangChain, no CrewAI, no OpenAI SDK. The whole thing is a Claude Code skill, one markdown file with a small YAML header (name, description, a version note) and instructions underneath. No orchestration framework, because there's nothing to orchestrate: one model, one file, one conversation.

Execution: no GitHub Actions, no webhook, no server. The real file lives in a folder on my machine. A symlink drops it into `~/.claude/skills/PRD` so any Claude Code session, in any project, can call it by typing `/PRD`. Editing the file changes its behavior on the very next call, no deploy step, no build.

It's a simple executable automation that anyone can build. Perfectly appropriate for hiring/HR workflow.

#### Step 3: the actual implementation

```markdown
---
name: PRD
description: |
  Pierre-Richard DUPONT interviews you about a prompt, brief, PRD ask, or implementation-plan
  request before you send it, asking the questions that catch a vague ask, a stated-vs-real
  problem gap, or an unverifiable spec before they turn into wasted work downstream.
---

## Step 1, classify the ask
1. Research ask
2. PRD or spec ask
3. PRD to plan ask
4. General prompt

## Step 2, run the matching interview
One round = one message. Ask 2 to 4 questions, wait, let the answer shape the next round.

How to phrase each question, cheapest format first:
1. Propose a default with your reasoning, ask to confirm or correct
2. Lettered options when the answer space is small
3. Open question, only when neither applies
```

That's not pseudocode standing in for the real thing. That markdown file, trimmed here for space, is the entire agent.

#### Best practices for small agents like this one

- **Propose, don't interrogate.** A blank question costs the user more than a guess they can correct. Default to a guess with reasoning attached, or a lettered menu. Open questions are the fallback, not the default.
- **One round at a time.** Never dump the whole question list. Answers should shape what gets asked next.
- **Self-check before declaring done.** Cool tips here: better to pause and ask itself whether it's still assuming something before it hands anything back.
- **Scope it down on purpose.** It skips the interview entirely on anything small, a typo fix doesn't need a four-lane classification. Small agents fail by doing too much, not too little.

### Where this leaves me

Routine has been shortcut at its extreme, by a simple /PRD in my prompt. It s like if you would go to gym, do 1 squat and everything would automatically follow with the state of art.

The skill file is sitting in my folder, if anyone wants a look, same offer as last time. PM me and I will share the README.

Enjoy the rest of your summer. Stay hydrated and don't forget your sunscreen.
