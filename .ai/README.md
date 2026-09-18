# AI rules for snowflake-jdbc

Three consumers, three trees. Copybara excludes `.ai/**`, `.cursor/**`, and `.claude/**` from the public mirror.

| Tree | Who reads it | When |
|------|----------------|------|
| `.ai/review/*.yaml` | Arctic Owl PR reviewer | After a PR is opened |
| `.cursor/rules/*.mdc` and `.cursor/skills/*/SKILL.md` | Cursor | While writing / on skill invocation |
| `.claude/rules/*.md` and `.claude/skills/*/SKILL.md` | Claude Code | While writing / on skill invocation |

`.ai/review/` is the source of truth for coding conventions. Cursor and Claude do **not** load those YAML files unless a rule or skill tells them to. That wiring lives in `.cursor/rules/apply-review-rulesets.mdc` and `.claude/rules/apply-review-rulesets.md`.

Canonical skill bodies live under `.claude/skills/<name>/SKILL.md`. Cursor skills are pointers to those files.
