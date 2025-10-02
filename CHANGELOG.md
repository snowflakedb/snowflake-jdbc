#### For all official JDBC Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc

# Changelog
- v3.26.2-SNAPSHOT
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Added HTTP 307 & 308 retries in case of internal IP redirects
    - Make PAT creation return `ResultSet` when using `execute` method
    - Renamed CRL_REVOCATION_CHECK_MODE to CERT_REVOCATION_CHECK_MODE in CLIENT_ENVIRONMENT metrics
    - Test coverage for multistatement jdbc.
    - Fixed permission check for .toml config file.
    - Bumped netty to 4.1.127.Final to address CVE-2025-58056 and  CVE-2025-58057
    - Added support for Interval Year-Month and Day-Time types in JDBC.