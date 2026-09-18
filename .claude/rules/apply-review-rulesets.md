# Apply Review Rulesets While Writing

The `.ai/review/*.yaml` files are this repo's authoritative coding conventions,
enforced by the AI Code Reviewer (Arctic Owl) **at PR time**. Apply them while
writing so the bot has nothing to flag.

Before editing or creating a file, read the `.ai/review/*.yaml` whose
`allowed_folders` + `allowed_file_extensions` match it (and not its
`excluded_folders`), and follow that ruleset's `rule:` blocks. Glob
`.ai/review/` to discover the current set rather than assuming a fixed list.

Examples:

- `src/test/java/**/*.java` → `jdbc-java.yaml`, `jdbc-flaky-tests.yaml`
- `src/main/java/**/*.java` → `jdbc-java-impl.yaml`, `jdbc-java.yaml`, `jdbc-logging.yaml`
- `.github/workflows/**/*.yml` → `jdbc-github-actions-allowlist.yaml`

A direct user instruction wins over these rulesets.

<!-- sync-target: .cursor/rules/apply-review-rulesets.mdc carries this body
     verbatim plus Cursor frontmatter. alwaysApply rules are injected into the
     agent system prompt at session start, so both files need full content. -->
