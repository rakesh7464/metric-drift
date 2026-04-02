# AI-Powered Development Workflow

Every part of this project was built using [Claude Code](https://claude.ai/claude-code) as the primary development environment — not just for code generation, but as an integrated workflow that carries context, enforces standards, and learns over time.

## Layered Project Instructions (CLAUDE.md)

Claude Code reads `CLAUDE.md` files at multiple levels, each adding specificity:

- **Global** (`~/.claude/CLAUDE.md`) — personal preferences: keep it simple, no unnecessary abstractions, only commit when asked
- **Workspace** (`~/personal/CLAUDE.md`) — conventions for all personal projects: flexible tech stack, self-contained dependencies
- **Project** (`metric-drift/CLAUDE.md`) — stack details, model descriptions, CI setup, Snowflake profile info

This means every conversation starts with full project awareness. No "remind me what framework we're using" — the AI already knows the dbt version, the Snowflake account, the model DAG, and the CI pipeline.

## Persistent Memory

Claude Code's memory system stores context across sessions:

- **User preferences** — role, expertise level, collaboration style
- **Feedback** — corrections that carry forward (e.g., "source ~/.zshrc before dbt commands" after hitting that issue once)
- **Project state** — what's been completed, what's next, key decisions and their rationale
- **References** — pointers to external systems (Snowflake trial details, GitHub config)

This is what makes multi-session projects work. The AI picks up exactly where we left off — no re-explaining the architecture or re-reading the codebase from scratch.

## Custom Slash Commands

Repetitive workflows are captured as reusable commands:

- `/commit` — stage, review changes, write a focused commit message
- `/review` — review current changes for quality and correctness
- `/test` — run the project's test suite
- `/snowflake` — help write or debug Snowflake SQL

These turn multi-step processes into one-liners and ensure consistency across sessions.

## Security Guardrails

AI access is scoped from the start with deny rules that block reads of:

- `.env`, credentials, secrets, tokens, passwords
- SSH keys, PEM files, AWS credentials
- `.netrc`, git-credentials

This isn't an afterthought — it's part of the initial setup before any code is written.

## What This Looks Like in Practice

Here's how metric-drift was built across a few sessions:

1. **Session 1** — Scaffolded the dbt project, wrote staging/intermediate/mart models, created synthetic seed data, genericized for public sharing, pushed to GitHub
2. **Session 2** — Connected to Snowflake trial, ran the full pipeline (seed → run → test), added GitHub Actions CI, built the Cortex AI insights layer — all end-to-end
3. **Session 3** — Cleaned up Snowflake, updated portfolio, drafted and published LinkedIn post

Each session started with "recall the portfolio plan" and the AI had full context immediately. Decisions made in session 1 informed behavior in session 3 without any repetition.

## Why This Matters

The analytics engineering role is changing. The tools we use to *build* are becoming as important as what we build. An AI-powered workflow isn't about writing code faster — it's about maintaining context across complex, multi-session projects, enforcing quality standards automatically, and spending less time on scaffolding so you can focus on the interesting problems.
