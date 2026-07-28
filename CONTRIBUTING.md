# Contributing to the Pathway Live Data Framework

Welcome! This guide is intended to help developers that are new to the community
to make contributions to the Pathway Live Data Framework project. Please be sure to also read our [code of conduct](CODE_OF_CONDUCT.md). We work hard to make this a welcoming community for all, and we're excited to have you on board!

## The basics

* Use the [Issue Tracker](https://github.com/pathwaycom/pathway/issues) to
  report bugs, crashes, performance issues, etc... Please include as much detail
  as possible.
* [Discord](https://discord.com/invite/pathway) is our main gathering place - jump in and ask us anything!
* [Our forum](https://discord.com/channels/1042405378304004156/1044276516290314381) is a great
  venue to ask questions, start a design discussion, or post an RFC.

## What helps us most: your ideas and your experience

Recent progress in AI tooling has made writing code dramatically cheaper. It has
not made *reviewing* code cheaper. Reading an unfamiliar change, understanding
how it interacts with the rest of the system, and taking responsibility for it
still costs a human reviewer the same attention it always did. Good code has, if
anything, become more valuable rather than less - but the scarce resources have
shifted, and today the two scarcest things in this project are review capacity
and a clear sense of which problem is worth solving next.

You can help most with the second one. **Opening an issue is the single most
valuable contribution you can make**, and it is the way we prefer to hear from
you. We would love to know:

* how you use Pathway Live Data Framework in your own project, and what your setup looks like;
* which features are missing, or which ones don't quite fit your use case;
* where you ran into friction, confusion, or a workaround you weren't happy with;
* bugs you hit, with as much detail as you can share;
* usage patterns that recur for you and might deserve first-class support;
* anything else about the implementation that you think we should know.

Once a problem is well described, implementing it on our side is usually fast: we
work with the latest frontier models on a codebase we know from the inside, and
we can iterate quickly. What we cannot generate on our own is your perspective -
the context of a real project using Pathway Live Data Framework in a way we haven't anticipated. That
is exactly the part we're asking you for.

## Contributing code

We are eager to build the Pathway Live Data Framework together with the community
and are excited to have you here!

Please send us a pull request if **either** of the following is true:

* **The change is small and self-contained** - a bug fix, a small correction, a
  docs improvement: something a reviewer can read end to end in one short sitting.
  Changes like these are easy for us to accept and usually land quickly.
* **You have explicit approval from a maintainer** to work on it. The way
  to get it is to file an issue or ask us on Discord and tell us what you'd like
  to build. You'll get design advice up front and a much faster review afterwards.

For anything larger, please start with an issue rather than with code. **If a
substantial pull request arrives without prior maintainer approval, we reserve the
right to close it without a detailed review** - not out of any ill will, but because
reconstructing the context around a large external change and validating it can take
more effort than solving the same problem from scratch with full knowledge of the
codebase. We'd very much like to avoid that outcome for work you've already put
effort into, which is exactly why we ask you to check with us first.

### Why we ask for this

We'd rather explain the reasoning than simply state the rule.

When a substantial change arrives from outside, the context that makes review fast
is missing. We don't know which tools and models were involved, what they were
asked to do, how the author evaluated the output, or how the change was reconciled
with the rest of the system. Reconstructing all of that from the diff alone
regularly takes longer than implementing the same feature ourselves would have.

This is not a judgment of AI-assisted work - we use these tools every day, and
we're glad you do too. It simply reflects where the bottleneck now sits, and we
think it's fairer to say so openly than to leave good-faith pull requests waiting
in a queue.

### Sharing how you worked

**Every substantial pull request should come with a short account of how it came
about** - what you were aiming for, how you satisfied yourself that it is correct,
and which parts you would most like a reviewer to scrutinize. This applies whatever
tools were or weren't involved: we won't ask anyone to prove anything about how a
change was produced, we just need the context a diff cannot carry on its own.

If you worked with AI tooling, sharing the session is often the easiest way to do
this. Most agents can export a transcript (`/export` in Claude Code, `/chat share`
in Gemini CLI, shared transcripts in Cursor, or the session file the tool keeps on
disk), and the prompts alone are already useful. Do read a transcript before
publishing it - they pick up environment variables, file contents and pasted
credentials, automatic redaction is best-effort at most, and a model may echo a
secret back in its own reply. If in doubt, a few honest sentences beat a leaked key.

For a **small fix** none of this matters much: send it without a word of context and
it will still be reviewed. For a **substantial contribution** it is a requirement -
a large pull request that arrives with no account of how it was made is unlikely to
be reviewed, and will usually be closed. This is not meant to diminish the effort
behind it. The cost of producing a change and the cost of understanding one have
simply drifted a long way apart, and without any context that whole gap lands on a
maintainer.

## How pull requests are handled

We use a standard GitHub fork + pull request model for merging and reviewing
changes. New pull requests should be made against the main upstream branch.
After a pull request is opened it will be reviewed, and merged after
passing continuous integration tests and being accepted by a project or
sub-system maintainer.

We maintain a [Changelog](https://github.com/pathwaycom/pathway/blob/main/CHANGELOG.md) where all notable changes to this project are documented. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

We ask that developers sign our [contributor license
agreement](https://cla-assistant.io/pathwaycom/pathway). The
process of signing the CLA is automated, and you'll be prompted with instructions
the first time you submit a pull request to the project.
