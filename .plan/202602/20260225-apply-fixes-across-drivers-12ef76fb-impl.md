# Implementation Plan

| Field | Value |
|-------|-------|
| Task | based on prs: commits: https://github.com/snowflakedb/snowflake-jdbc/commit/5f50c3702d699d66ccf06e4b419e0cc927ea7ef2 and https://github.com/snowflakedb/snowflake-jdbc/commit/a633a1830c1618818e8bcada7a... |
| Date | 2026-02-25 |
| Agent | task-12ef76fb |
| Repository | snowflakedb/snowflake-jdbc |
| PRs | 4 |

## Overview

Based on the JDBC commits analysis, 4 out of 6 driver repositories need changes. Each repository gets one PR since they are independent codebases. The changes are:

1. **gosnowflake**: Apply both fixes (ci/test.sh docker network changes + Jenkinsfile svn_revision change from 'bptp-stable' to 'temptest-deployed')
2. **snowflake-connector-net**: Apply both fixes (ci/test.sh docker network changes + Jenkinsfile svn_revision change from 'bptp-stable' to 'temptest-deployed')
3. **snowflake-connector-nodejs**: Special handling needed - has custom docker network setup with proxy, needs investigation if changes apply
4. **snowflake-odbc**: Apply both fixes (ci/test.sh docker network changes + Jenkinsfile svn_revision change from 'bptp-stable' to 'temptest-deployed')

**Repositories NOT needing changes:**
- **snowflake-connector-python**: Already uses --network=host in test_docker.sh, no IP_ADDR extraction or --add-host flags. Jenkinsfile dynamically computes bptp-stable tag (different pattern).
- **libsnowflakeclient**: No ci/test.sh file exists, Jenkinsfile doesn't have svn_revision parameter.

Each PR is small (~10-20 lines) and under the 600-line limit.

## PR Stack

### PR 1: SNOW-3071137 Update CI for uSUT Jenkins execution

**Description**: Updates CI configuration to support test execution on uSUT (micro-SUT) on Jenkins, matching changes from snowflake-jdbc.

## Changes
- Remove IP_ADDR extraction from ci/test.sh
- Remove --add-host flags for *.reg.local domains in docker run command
- Add --network=host flag to docker run command
- Update svn_revision parameter in Jenkinsfile from 'bptp-stable' to 'temptest-deployed'

## Context
These changes align the gosnowflake CI with the JDBC driver's CI improvements for running tests on Jenkins uSUT environment.

Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/5f50c3702d699d66ccf06e4b419e0cc927ea7ef2
Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/a633a1830c1618818e8bcada7a21859ebd09933e

**Scope**:
**Repository: snowflakedb/gosnowflake**

1. Modify ci/test.sh:
   - Remove lines 17-18 that extract IP_ADDR using /sbin/ip command
   - In the docker container run command (around line 42), add --network=host flag right after --rm flag
   - Remove the --add-host=snowflake.reg.local:${IP_ADDR} and --add-host=s3testaccount.reg.local:${IP_ADDR} flags from docker run command

2. Modify Jenkinsfile:
   - Change line 15: string(name: 'svn_revision', value: 'bptp-stable') to string(name: 'svn_revision', value: 'temptest-deployed')

Expected diff: ~10-15 lines total

**Rationale**: Independent repository requiring the same CI fixes as JDBC driver

---

### PR 2: SNOW-3071137 Update CI for uSUT Jenkins execution

**Description**: Updates CI configuration to support test execution on uSUT (micro-SUT) on Jenkins, matching changes from snowflake-jdbc.

## Changes
- Remove IP_ADDR extraction from ci/test.sh
- Remove --add-host flags for *.reg.local domains in docker run command
- Add --network=host flag to docker run command
- Update svn_revision parameter in Jenkinsfile from 'bptp-stable' to 'temptest-deployed'

## Context
These changes align the .NET connector CI with the JDBC driver's CI improvements for running tests on Jenkins uSUT environment.

Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/5f50c3702d699d66ccf06e4b419e0cc927ea7ef2
Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/a633a1830c1618818e8bcada7a21859ebd09933e

**Scope**:
**Repository: snowflakedb/snowflake-connector-net**

1. Modify ci/test.sh:
   - Remove lines 9-10 that extract IP_ADDR using /sbin/ip command
   - In the docker container run command (around line 33), add --network=host flag right after --rm flag
   - Remove the --add-host=snowflake.reg.local:${IP_ADDR} and --add-host=s3testaccount.reg.local:${IP_ADDR} flags from docker run command

2. Modify Jenkinsfile:
   - Change line 36: string(name: 'svn_revision', value: 'bptp-stable') to string(name: 'svn_revision', value: 'temptest-deployed')

Expected diff: ~10-15 lines total

**Rationale**: Independent repository requiring the same CI fixes as JDBC driver

---

### PR 3: SNOW-3071137 Update Jenkinsfile svn_revision for uSUT

**Description**: Updates Jenkinsfile to use 'temptest-deployed' tag for uSUT Jenkins execution, matching changes from snowflake-jdbc.

## Changes
- Update svn_revision parameter in Jenkinsfile from 'bptp-stable' to 'temptest-deployed'

## Context
The Node.js connector uses a custom docker network setup with proxy in ci/test.sh, which differs from the JDBC pattern. Only the Jenkinsfile svn_revision change is applicable.

Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/a633a1830c1618818e8bcada7a21859ebd09933e

**Scope**:
**Repository: snowflakedb/snowflake-connector-nodejs**

1. Modify Jenkinsfile:
   - Change line 41: string(name: 'svn_revision', value: 'bptp-stable') to string(name: 'svn_revision', value: 'temptest-deployed')

Note: ci/test.sh is NOT modified because it uses a custom network setup (--net $NETWORK_NAME with proxy configuration) that is fundamentally different from the JDBC pattern. The network=host change would break the proxy testing setup.

Expected diff: 2 lines (1 removal, 1 addition)

**Rationale**: Node.js connector has unique network configuration with proxy testing, only Jenkinsfile change applies

---

### PR 4: SNOW-3071137 Update CI for uSUT Jenkins execution

**Description**: Updates CI configuration to support test execution on uSUT (micro-SUT) on Jenkins, matching changes from snowflake-jdbc.

## Changes
- Remove IP_ADDR extraction from ci/test.sh
- Remove --add-host flags for *.reg.local domains in docker run command
- Add --network=host flag to docker run command
- Update svn_revision parameter in Jenkinsfile from 'bptp-stable' to 'temptest-deployed'

## Context
These changes align the ODBC connector CI with the JDBC driver's CI improvements for running tests on Jenkins uSUT environment.

Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/5f50c3702d699d66ccf06e4b419e0cc927ea7ef2
Ref: https://github.com/snowflakedb/snowflake-jdbc/commit/a633a1830c1618818e8bcada7a21859ebd09933e

**Scope**:
**Repository: snowflakedb/snowflake-odbc**

1. Modify ci/test.sh:
   - Remove lines 30-31 that extract IP_ADDR using /sbin/ip command
   - In the docker container run command (around line 55), add --network=host flag right after --rm flag
   - Remove all --add-host flags from docker run command: --add-host=snowflake.reg.local:${IP_ADDR}, --add-host=s3testaccount.reg.local:${IP_ADDR}, --add-host=azureaccount.reg.local:${IP_ADDR}, --add-host=gcpaccount.reg.local:${IP_ADDR}

2. Modify Jenkinsfile:
   - Change line 114: string(name: 'svn_revision', value: 'bptp-stable') to string(name: 'svn_revision', value: 'temptest-deployed')

Expected diff: ~12-18 lines total

**Rationale**: Independent repository requiring the same CI fixes as JDBC driver
