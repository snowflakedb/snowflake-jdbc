#### For all official JDBC Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc

# Changelog
- v3.27.1-SNAPSHOT
    - Added platform detection on login to set PLATFORM metric in CLIENT_ENVIRONMENT
    - Disable DatabaseMetaDataLatestIT::testUseConnectionCtx test
    - Fix IT tests to construct OAuth scopes correctly
    - Fix exponential backoff retry time for non-auth requests
    - Upgrade aws-sdk to 1.12.792 and add STS dependency
    - Add rockylinux9 CI tests as part of RHEL 9 support
    - Bumped grpc-java to 1.76.0 to address CVE-2025-58056 from transient dep
    - Added `workloadIdentityImpersonationPath` config option for `authenticator=WORKLOAD_IDENTITY` allowing workloads to authenticate as a different identity through transitive service account impersonation (snowflakedb/snowflake-jdbc#2348)
    - Added support for authentication as a different identity through transitive IAM role impersonation for AWS (snowflakedb/snowflake-jdbc#2364)
    - Add AWS identity detection with ARN validation

- v3.27.0
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Added HTTP 307 & 308 retries in case of internal IP redirects
    - Make PAT creation return `ResultSet` when using `execute` method
    - Renamed CRL_REVOCATION_CHECK_MODE to CERT_REVOCATION_CHECK_MODE in CLIENT_ENVIRONMENT metrics
    - Test coverage for multistatement jdbc.
    - Fixed permission check for .toml config file.
    - Bumped netty to 4.1.127.Final to address CVE-2025-58056 and  CVE-2025-58057
    - Add support for x-snowflake-session sticky HTTP session header returned by Snowflake
    - Added support for Interval Year-Month and Day-Time types in JDBC.
    - Added support for Decfloat types in JDBC.
    - Fixed pattern search for file when QUOTED_IDENTIFIERS_IGNORE_CASE enabled
    - Added support for CRL (certificate revocation list).