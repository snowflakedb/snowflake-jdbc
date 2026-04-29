---
name: graphite-pr-workflow
description: Create branches, commits, and pull requests using Graphite CLI (gt) for the snowflake-jdbc repository. Use when the user asks to create a PR, submit a PR, commit changes, or push a branch. Covers Graphite commands, SNOW-ticket commit message conventions, PR description templates, and snowflake-jdbc-specific pre-commit formatting/checkstyle requirements.
---

# Graphite PR Workflow (snowflake-jdbc)

## CRITICAL: Do Not Commit or Submit Without Explicit Instruction

**NEVER run `gt commit create`, `git commit`, `gt submit`, or open a PR unless the user has explicitly asked you to.** Completing a feature is NOT permission to commit. Passing tests is NOT permission to commit. Pre-commit checks succeeding is NOT permission to commit.

When you have finished implementing a feature or change, you MAY ask the user whether they want you to proceed with creating a commit and PR. Phrase it as a question and wait for explicit confirmation before doing anything that mutates git history or the remote.

Examples of acceptable prompts after finishing work:
- "The change is implemented and tests pass. Would you like me to format, run checkstyle, and create a commit + PR?"
- "Should I proceed with `gt commit create` and `gt submit`?"

Do NOT interpret ambiguous responses ("looks good", "great", "thanks") as approval to commit. Wait for an explicit "yes, commit" / "create the PR" / "go ahead and submit".

## Required Pre-Commit Checks (snowflake-jdbc)

Before running ANY commit command (`gt commit create`, `git commit`, `gt commit amend`), you MUST run the following two Maven commands in order from the repo root and confirm they succeed:

1. **Format the code** with the Spotify fmt plugin:

   ```bash
   mvn com.spotify.fmt:fmt-maven-plugin:format
   ```

2. **Validate checkstyle** passes cleanly:

   ```bash
   mvn clean validate --batch-mode --show-version -P check-style
   ```

If either command fails, fix the reported issues (re-running the formatter for any new files, or addressing checkstyle violations manually) and re-run both commands until both succeed. Only then may you proceed with committing — and only if the user has explicitly asked you to commit (see section above).

## Commit Message Convention

Prefix with Jira ticket: `SNOW-{ticket}: {description}`

```
SNOW-3254915: Add QueryStatus enum and is_still_running/is_an_error static methods
```

Use `NO-SNOW:` for changes without a ticket.

## Creating a PR (only after explicit user approval)

```bash
# 1. Create a tracked branch
gt branch create {user}/{branch-name}

# 2. Format and validate (REQUIRED before staging/committing — see Pre-Commit Checks above)
mvn com.spotify.fmt:fmt-maven-plugin:format
mvn clean validate --batch-mode --show-version -P check-style

# 3. Stage files
git add <files>

# 4. Commit (runs pre-commit hooks)
gt commit create -m "SNOW-{ticket}: {description}"

# 5. Push and create PR (draft mode in non-interactive)
gt submit --no-edit
```

Then update title/description via `gh pr edit` or the Graphite web UI.

## PR Description Template

```markdown
## Summary
- Bullet points describing what changed and why

## Context
Brief explanation of how this fits into the larger effort (e.g., "first of two PRs for...")

## Test plan
Explain how the change was tested: what commands were run, what was
verified, and the results. Be specific to this PR.
```

## Key Commands

| Command | Purpose |
|---------|---------|
| `mvn com.spotify.fmt:fmt-maven-plugin:format` | Auto-format Java sources (REQUIRED before commit) |
| `mvn clean validate --batch-mode --show-version -P check-style` | Run checkstyle validation (REQUIRED before commit) |
| `gt branch create {user}/{branch-name}` | Create branch tracked by Graphite |
| `gt commit create -m "..."` | Commit with message |
| `gt commit amend` | Amend current commit |
| `gt submit --no-edit` | Push and create/update PR |
| `gt submit --stack` | Submit entire stack of PRs |
| `gt restack` | Rebase stack after upstream changes |

## Before Creating a PR: Required Checks

Before proceeding with branch creation or committing, you MUST:

1. **Confirm the user has explicitly requested a commit/PR.** See the top of this file. Do not commit proactively.

2. **Run the formatter and checkstyle commands** described in the Required Pre-Commit Checks section above and confirm both pass.

3. **Ask for SNOW ticket number** if the user hasn't provided one. Use AskQuestion:
   - Prompt: "What is the SNOW Jira ticket number for this change?"
   - Options: a text-free response, or "NO-SNOW (no ticket)"
   Do NOT proceed with a commit until the ticket number is confirmed.

4. **Detect stacked PR situation.** Run `git log --oneline main..HEAD` or check `git branch --show-current` and its parent. If the current branch is NOT based directly off `main` (i.e., there are intermediate branches), ask:
   - "This branch is based off `{parent_branch}`, not `main`. Should this be a stacked PR?"
   - If yes: use `gt submit --stack` to submit the full stack
   - If no: use `gt submit --no-edit` for just this branch

## Notes

- `gt submit --no-edit` creates PRs in **draft mode** when non-interactive
- Pre-commit hooks run automatically on `gt commit create`, but they are NOT a substitute for running the formatter + checkstyle commands manually beforehand
