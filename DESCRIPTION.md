#### For the official JDBC Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc

# Changelog
- v3.26.2-SNAPSHOT
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Added HTTP 307 & 308 retries in case of internal IP redirects
    - Make PAT creation return `ResultSet` when using `execute` method
    - Renamed CRL_REVOCATION_CHECK_MODE to CERT_REVOCATION_CHECK_MODE in CLIENT_ENVIRONMENT metrics
    - Added platform detection on login to set PLATFORM metric in CLIENT_ENVIRONMENT