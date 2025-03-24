# Overview

SNOW-XXXXX

## Pre-review self checklist
- [ ] PR branch is updated with all the changes from `master` branch
- [ ] The code is correctly formatted (run `mvn -P check-style validate`)
- [ ] New public API is not unnecessary exposed (run `mvn verify` and inspect `target/japicmp/japicmp.html`)
- [ ] The pull request name is prefixed with `SNOW-XXXX: `
- [ ] Code is in compliance with internal logging requirements

## External contributors - please answer these questions before submitting a pull request. Thanks!

1. What GitHub issue is this PR addressing? Make sure that there is an accompanying issue to your PR.

   Issue: #NNNN


2. Fill out the following pre-review checklist:

   - [ ] I am adding a new automated test(s) to verify correctness of my new code
   - [ ] I am adding new logging messages
   - [ ] I am modifying authorization mechanisms
   - [ ] I am adding new credentials
   - [ ] I am modifying OCSP code
   - [ ] I am adding a new dependency or upgrading an existing one
   - [ ] I am adding new public/protected component not marked with `@SnowflakeJdbcInternalApi` (note that public/protected methods/fields in classes marked with this annotation are already internal)

3. Please describe how your code solves the related issue.

   Please write a short description of how your code change solves the related issue.
