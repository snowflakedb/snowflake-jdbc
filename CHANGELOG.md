#### For all official JDBC Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc

# Changelog
- v4.3.3-SNAPSHOT
  - Fixed the self-contained JAR shipping the non-gRPC-shaded Netty native libraries (`libnetty_transport_native_epoll_*`, `libnetty_transport_native_kqueue_*`, `libnetty_resolver_dns_native_macos_*`) unshaded, which caused an `UnsatisfiedLinkError` when they conflicted with a user's own Netty on the classpath. These libraries are now relocated with the `libnet_snowflake_client_jdbc_internal_netty_*` prefix to match the relocated `io.netty` package (snowflakedb/snowflake-jdbc#2705).
  - Bumped `google-cloud-storage` from 2.44.1 to 2.69.0, with all required transitive dependency version updates (`google-cloud-core`, `google-api-grpc`, `google-auth-library`, `google-http-client`, `gax`, `protobuf`, `guava`, `slf4j`, and others) (snowflakedb/snowflake-jdbc#2691).
  - Changed minimum heartbeat interval to 15 minutes (snowflakedb/snowflake-jdbc#2708).

- v4.3.2
  - Fixed `RestRequest` logging retryable, temporal non-200 responses as `ERROR` (now: `WARN`), and fixed `SnowflakeChunkDownloader` using flat, short jitter between retries (now uses `DecorrelatedJitterBackoff(1 s, 16 s)` like http requests) (snowflakedb/snowflake-jdbc#2693).
  - Fixed GCS PUT operations not retrying on transient errors (e.g. HTTP 503) despite `putGetMaxRetries` being configured (snowflakedb/snowflake-jdbc#2688).
  - Bumped jackson-databind to 2.18.9 from 2.18.7 (snowflakedb/snowflake-jdbc#2669 and snowflakedb/snowflake-jdbc#2690).
  - Bumped netty to 4.1.136.Final which addresses several vulnerabilities (snowflakedb/snowflake-jdbc#2690). 
  - Fixed snowflake-jdbc writing a `snowflake-minicore-*` temp directory and loading the native library at driver class-load time even when the driver was never used (e.g. when present on the classpath only as a transitive dependency). Minicore now loads lazily when the first Snowflake connection is created (`ConnectionFactory.createConnection`) instead of during `DriverInitializer.initialize()` (snowflakedb/snowflake-jdbc#2670).
  - Restored `GetCallerIdentity` as the default AWS Workload Identity Federation attestation method to avoid breaking existing users who have not configured the `ISSUER` in their Snowflake WIF setup. The `GetWebIdentityToken` (outbound JWT) flow introduced in v4.3.0 is now opt-in via the new `workloadIdentityAwsUseOutboundToken` connection property.
  - Fixed flaky `SnowflakeDriverIT.testDBMetadata`: the SHOW-based `getTables` metadata lookup could transiently return no rows right after the test table was created (metadata cache lag on shared/loaded accounts), so the lookup is now retried until the table becomes visible, and on very slow CI runners where propagation exceeds the retry window the test is skipped rather than failed (the mapping is covered deterministically by `DatabaseMetadataWiremockLatestIT`). Test-only change with no driver behavior impact (snowflakedb/snowflake-jdbc#2673).
  - Fixed flaky `DatabaseMetaDataLatestIT.testUnderscoreInSchemaNamePatternForPrimaryAndForeignKeys[WithPatternSearchDisabled]`: the SHOW-based `getPrimaryKeys`/`getImportedKeys` lookups could transiently return fewer rows than expected right after the table DDL (eventually-consistent PK/FK constraint metadata), so the lookups are now retried until the expected rows become visible. Test-only change with no driver behavior impact (snowflakedb/snowflake-jdbc#2673).
  - Fixed flaky `DatabaseMetaDataIT.testGetPrimarykeys` and `SnowflakeDriverIT.testConstraints` (eventually-consistent PK/FK constraint metadata after DDL) by retrying the constraint metadata lookups until visible. Replaced the live, concurrent stress tests `DatabaseMetaDataLatestIT.test[No]PatternSearchAllowedForPrimaryAndForeignKeys` (which timed out on slow CI runners) with deterministic coverage in the new `DatabaseMetadataWiremockLatestIT`, which validates the `getTables`/`getPrimaryKeys`/`getImportedKeys` SHOW-result mapping and the `enablePatternSearch` pattern-vs-literal behavior using stubbed responses (no live account or metadata-propagation dependency). Test-only change with no driver behavior impact (snowflakedb/snowflake-jdbc#2673).
  - Fixed the Loader API (`StreamLoader.setVectorColumns`) throwing `Loader$ConnectionError: ... Result set has been closed` when the `getColumns` metadata lookup returned no rows. The result set returned by `getColumns` closes itself once `next()` runs out of matching rows, so the unchecked `rs.next()` followed by `rs.getString(...)` raised "Result set has been closed"; the result is now read only when `rs.next()` returns a row. This intermittently aborted concurrent loads (e.g. `FlatfileReadMultithreadIT`) when the `SHOW COLUMNS` metadata query raced with concurrent DML on the same table (snowflakedb/snowflake-jdbc#2674).
  - Fixed `uploadStream`/`downloadStream` failing with SQL compilation errors when the stage reference contains non-ASCII characters (e.g. Japanese schema names). Stage references in internally generated PUT/GET commands are now wrapped in single quotes when they contain characters that require quoting per Snowflake SQL syntax (SNOW-3713887).
  - Fixed `authenticator=externalbrowser` login crashing with `StringIndexOutOfBoundsException` when the browser opened an empty preconnect socket or delivered a large SSO request across multiple reads; the localhost callback server now ignores empty connections and reassembles fragmented requests, including POST bodies that arrive in a TCP segment separate from the headers (snowflakedb/snowflake-jdbc#2687).
  - Fixed a permanent HTTP connection pool slot leak in `RestRequest.executeWithRetries`: on the retry-exhaustion break with no response or a non-200 status, the underlying connection is now released back to the pool (mirroring the retry path), preventing the driver from wedging on the exhausted pool under sustained network failures such as repeated 503s on chunk downloads (snowflakedb/snowflake-jdbc#2643).
  - Bumped grpc-java to 1.82.2 (snowflakedb/snowflake-jdbc#2696).

- v4.3.1
  - Fixed GCS-backed internal stage PUT failing with opaque `invalid_gcs_credentials` in SPCS pods on GCP: the GCS SDK's Application Default Credentials (ADC) probe was reaching out to `metadata.google.internal` which is unreachable inside SPCS; explicit credentials are now always set when a `GCS_ACCESS_TOKEN` is present, suppressing the ADC probe entirely. Also fixed `GCSAccessStrategyAwsSdk` rejecting custom GCS endpoints that lack an `https://` scheme prefix (e.g. bare `storage.me-central2.rep.googleapis.com`), mirroring the existing handling in `GCSDefaultAccessStrategy`. The catch-all in `setupGCSClient` now chains and logs the original exception instead of swallowing it (snowflakedb/snowflake-jdbc#2664).
  - Fixed Azure PUT memory leak where each PUT instantiated a fresh `BlobServiceClient` whose underlying reactor-netty stack the SDK exposes no API to release; the Azure SDK `HttpClient` and its `ConnectionProvider` are now shared across all PUTs in a session and disposed at session close, mirroring the existing S3 pattern (snowflakedb/snowflake-jdbc#2658).
  - Fixed `SFResultJsonParser2Failed: invalid escaped unicode character` when a chunked JSON result contained UTF-16 surrogate-pair `\u` escapes (e.g. emoji) and the read buffer happened to split exactly 9 bytes after `\u`; the off-by-one boundary guard in `ResultJsonParserV2` now reserves the full 10 bytes a surrogate pair requires (snowflakedb/snowflake-jdbc#2660).
  - Fixed (by removing) stale `com.amazonaws.util.Base16/Base64` bytecode references from the shaded JAR by excluding dead `SFBinary` and `SFBinaryFormat` classes from the bundled `snowflake-common` artifact. Security scanners shold no longer flag `snowflake-jdbc-thin` as containing AWS SDK v1 references. (snowflakedb/snowflake-jdbc#2665).
  - Bumped grpc-java to 1.82.0 from 1.81.0 (snowflakedb/snowflake-jdbc#2663).


- v4.3.0
    - Bumped AWS SDK from 2.37.5 to 2.45.1, which transitively brings netty up to 4.1.133.Final and resolves a cluster of High/Medium netty CVEs (HTTP request smuggling, CRLF injection, data amplification, resource allocation) flagged by Snyk against `netty-nio-client` in `thin_public_pom.xml` (snowflakedb/snowflake-jdbc#2654).
    - Bumped jackson to 2.18.7 to address two High-severity resource-exhaustion CVEs in jackson-core 2.18.4.1, and added a `.snyk` policy file with justified ignores for the dual-licensed `javax.servlet-api` / `javax.annotation-api` findings and the tika-core XXE (`SNYK-JAVA-ORGAPACHETIKA-14188255`), which has no Java-8-compatible fix and is not reachable through the driver's only Tika caller (snowflakedb/snowflake-jdbc#2654).
    - Fixed OAuth token requests sending `scope=session:role:null` when no scope is configured (or scope is empty/blank); the `scope` parameter is now omitted entirely in those cases (snowflakedb/snowflake-jdbc#2646).
    - Fixed Okta native SSO federated login sending malformed JSON to `/api/v1/authn` (HTTP 400 from Okta) when the username or password contained JSON-special characters such as double quotes or backslashes; the request body is now serialized with Jackson instead of string concatenation.
    - Added one in-band telemetry record per successful login describing which connection-identifier fields the user supplied (`account_provided`, `account_with_region`, `account_org_provided`, `region_provided`, `host_provided`). No hostname or account value is included. This is gated by the existing server-side `CLIENT_TELEMETRY_ENABLED` parameter and can additionally be disabled locally by setting `SF_TELEMETRY_DISABLE_CONNECTION_SHAPE=true`. The telemetry collection is time-boxed and will be removed in a future release.
    - Fixed `SnowflakeChunkDownloader` per-chunk metrics log misattributing the response body transfer to `parseTime`: `getResultStreamProvider().getInputStream()` returns once headers come back and streams the body lazily during the parser's `read()` calls, so the old code billed only HTTP/TLS setup to `downloadTime` and the entire body read+parse to `parseTime`. When the metrics logger is at `FINE`/debug level, the InputStream is now wrapped in a `TimingInputStream` that accumulates time blocked inside `read()`, so `downloadTime` reflects true network read time and `parseTime` reflects only CPU parse cost; at higher log levels the original stream is used unchanged to avoid the per-`read()` overhead (snowflakedb/snowflake-jdbc#2640).
    - Fixed `Connection.isValid()` silently swallowing thread interruption: when the underlying heartbeat is interrupted, the connection's interrupt flag is now restored via `Thread.currentThread().interrupt()` so connection pools and Thread shutdown mechanisms can react to the interruption (snowflakedb/snowflake-jdbc#2314).
    - Fixed non-retryable HTTP 400 response bodies always being logged as `"Failed to read content due to exception: Attempted read from closed stream"`. The response entity is now buffered before `RestRequest#checkForDPoPNonceError` and `SnowflakeUtil#logResponseDetails` consume it so both readers see the body (snowflakedb/snowflake-jdbc#2631).
    - Added defense-in-depth canonical-path validation in the S3, Azure, and GCS download clients to ensure resolved local download paths cannot escape the user's GET target directory via traversal segments, absolute paths, or symlink redirection (snowflakedb/snowflake-jdbc#2623).
    - Fixed path traversal via server-controlled filenames in `SnowflakeFileTransferAgent` GET destination filename derivation; backslash separators are now stripped and traversal/absolute basenames are rejected (snowflakedb/snowflake-jdbc#2622).
    - Further changes regarding auto-configuration (`jdbc:snowflake:auto` style connection config) (snowflakedb/snowflake-jdbc#2625):
      - Fixed bug leading to `'Connection property specified more than once: DB'` error, when both `connections.toml` (`database`) and JDBC URL (`db`) defined database 
      - Enhancement: now parameters passed as `Properties()` are also considered when building connection. For conflicting items defined in multiple places, priority is: Properties > JDBC URL > `connections.toml`
      - Enhancement (supportability): added provenance tracking for config keys and log them once per connection on debug level
    - Fixed IllegalStateException when creating new Snowflake connections during JVM shutdown (SIGTERM); HeartbeatRegistry now skips heartbeat registration gracefully instead of throwing (snowflakedb/snowflake-jdbc#2617).
    - Fixed auto-config debug log messages (provenance, TOML parsing) not appearing in `client_config_file`-governed log file; messages are now replayed after logger initialization so they reach the FileHandler (snowflakedb/snowflake-jdbc#2632).
    - The AWS S3 client now reuses a per-session shared Netty `SdkEventLoopGroup`, torn down once at session close, eliminating Netty's 2 s `shutdownGracefully` quiet period previously paid on every per-PUT/GET client close (snowflakedb/snowflake-jdbc#2620).
    - Bumped netty to 4.1.135.Final which addresses several vulnerabilities  (snowflakedb/snowflake-jdbc#2655). 
    - Fixed inverted null check in `CredentialManager.updateInputWithTokenAndPublicKey` that prevented DPoP bundled access tokens loaded from the credential cache from being applied to the login input (snowflakedb/snowflake-jdbc#2650).
    - Fixed `Connection.setCatalog` and `Connection.setSchema` producing malformed SQL (or switching to an unintended database/schema) when the supplied name contained an embedded `"` character; the name is now escaped per the SQL-standard quoted-identifier rule before being interpolated into the `USE` statement (snowflakedb/snowflake-jdbc#2651).
    - Switched AWS Workload Identity Federation attestation from a SigV4-presigned `GetCallerIdentity` request to STS `GetWebIdentityToken`, returning a signed JWT directly. (snowflakedb/snowflake-jdbc#2653)

- v4.2.0
    - Extended the `SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION` environment variable to also bypass permission verification on the `connections.toml` config file and on the credential cache file (`credential_cache_v1.json`), unblocking driver use in SPCS environments where strict 0600/0700 ownership cannot be guaranteed (snowflakedb/snowflake-jdbc#2614)
    - Fixed NPE in `RestRequest.sendIBHttpErrorEvent` when `SFSession.getTelemetryClient()` returns null because the session URL is not yet set; a `NoOpTelemetryClient` is now returned instead, allowing the original HTTP error to be surfaced to the caller (snowflakedb/snowflake-jdbc#2610)
    - Added support for attaching the SPCS service-identifier token (`SPCS_TOKEN`) to login requests when the driver is running inside an SPCS container (gated on the `SNOWFLAKE_RUNNING_INSIDE_SPCS` environment variable; token read from `/snowflake/session/spcs_token`) (snowflakedb/snowflake-jdbc#2603)
    - Added libc family and version detection (`LIBC_FAMILY`, `LIBC_VERSION`) to the `CLIENT_ENVIRONMENT` section of the login request on Linux (snowflakedb/snowflake-jdbc#2596)
    - Fixed NPE in `SFTrustManager.validateRevocationStatusMain` when the OCSP cache contains a non-SUCCESSFUL response (e.g. `unauthorized(6)`); the response is now surfaced as an `SFOCSPException` so cache eviction and fail-open run normally (snowflakedb/snowflake-jdbc#2597)
    - Added IPv6 support for cloud metadata services so Workload Identity Federation and platform detection work on IPv6-only instances (snowflakedb/snowflake-jdbc#2586):
      - GCP WIF attestation now uses hostname `metadata.google.internal` instead of the IPv4 link-local address.
      - EC2 instance detection probes the IPv4 and IPv6 IMDS endpoints (`[fd00:ec2::254]`) in parallel so detection succeeds on IPv6-only instances without doubling the detection budget on dual-stack hosts.
    - Added `enableCopyResultSet` connection property (default `false`): when `true`, `Statement.execute()` exposes the COPY INTO per-file metadata result set via `getResultSet()` instead of consuming it internally (snowflakedb/snowflake-jdbc#2592)
    - Migrated CI test images from CentOS 7 (EOL) to Rocky Linux 8 (snowflakedb/snowflake-jdbc#2578)
    - Fixed NPE "The URI scheme of endpointOverride must not be null" happening during file transfer (e.g. PUT) in some use-cases (snowflakedb/snowflake-jdbc#2572)
    - Fixed connections.toml auto-configuration behaviour (snowflakedb/snowflake-jdbc#2591):
      - now defaulting to port 443 instead of 80 when neither port nor protocol is specified
      - config coming from the JDBC connection string are no longer ignored when auto-configuration sourced items also present (when both present, direct connection config takes precedence)
    - Fixed protocol field in connections.toml being ignored, causing connections to always use HTTPS (snowflakedb/snowflake-jdbc#2585)
    - Fixed SecurityException on credential cache file ownership check in containers where JVM returns '?' for user.name (snowflakedb/snowflake-jdbc#2600).
    - Fixed credential cache delete operations ignoring clientStoreTemporaryCredential=false setting (snowflakedb/snowflake-jdbc#2600).
    - Fixed S3 transfer thread pool leak during repeated PUT/GET operations causing possible OOM (snowflakedb/snowflake-jdbc#2602).
    - Bumped BouncyCastle to 1.84 to address CVE-2026-0636, CVE-2026-5588, and CVE-2026-5598 (snowflakedb/snowflake-jdbc#2593).
    - Added `workloadIdentityAwsExternalId` connection property to support AWS STS external ID in Workload Identity Federation role-chaining flows (snowflakedb/snowflake-jdbc#2565).
    - Bumped grpc-java to 1.81.1 now that they also upgraded to netty 4.1.132.Final as the second part of PR 2561, and also netty itself to 4.1.133.Final to address several CVE (snowflakedb/snowflake-jdbc#2611).

- v4.1.0
    - Added warning about using plain HTTP OAuth endpoints (snowflakedb/snowflake-jdbc#2556).
    - Fix initializing ObjectMapper when DATE_OUTPUT_FORMAT is specified (snowflakedb/snowflake-jdbc#2545).
    - Fix Netty native library conflict in thin JAR (snowflakedb/snowflake-jdbc#2559)
    - Bumped netty to 4.1.132.Final to address CVE-2026-33870 (High) and CVE-2026-33871 (High) (snowflakedb/snowflake-jdbc#2561)
    - Added getRole, getWarehouse and getDatabase API extension methods (snowflakedb/snowflake-jdbc#2564)
    - Fix driver failure when security manager prohibits access to system properties, environment variables and modifying security providers (snowflakedb/snowflake-jdbc#2563)
    - Removed the io.netty.tryReflectionSetAccessible system property setting as it's no longer needed with modern Arrow/Netty versions (snowflakedb/snowflake-jdbc#2563)
    - Fixed crash in getColumns operation when table contained unrecognised column type (snowflakedb/snowflake-jdbc#2568).
    - Fixed session expiration when multiple sessions have different heartbeat intervals (snowflakedb/snowflake-jdbc#2566).
    - Merge QueryContext from failed query responses (snowflakedb/snowflake-jdbc#2570)

- v4.0.2
    - Fix expired session token renewal when polling results (snowflakedb/snowflake-jdbc#2489)   
    - Fix missing minicore async initialization that was dropped during public API restructuring in v4.0.0 (snowflakedb/snowflake-jdbc#2501)
    - Adjust level of logging during Driver initialization (snowflakedb/snowflake-jdbc#2504)
    - Add sanitization for nonProxyHosts RegEx patterns (snowflakedb/snowflake-jdbc#2506)
    - Fix bug with malformed file during S3 upload (snowflakedb/snowflake-jdbc#2502)
    - Added periodic closure of sockets closed by the remote end (snowflakedb/snowflake-jdbc#2481).
    - Add internal API usage telemetry tracker (snowflakedb/snowflake-jdbc#2509)
    - Change S3 Client's multipart threshold to 16MB (snowflakedb/snowflake-jdbc#2526)
    - Fixed fat jar with S3 iteration, the problem of not finding class `software.amazon.awssdk.transfer.s3.internal.ApplyUserAgentInterceptor` (snowflakedb/snowflake-jdbc#2519).
    - Removed Conscrypt from shading to prevent `failed to find class org/conscrypt/CryptoUpcalls` native error (snowflakedb/snowflake-jdbc#2519).
    - Add logging implementation to CLIENT_ENVIRONMENT telemetry (snowflakedb/snowflake-jdbc#2527)
    - Fix NPE when HOME directory cache is not available (snowflakedb/snowflake-jdbc#2534)
    - Bumped `commons-compress` dependency to latest (1.28.0) to address CVE-2024-25710 and CVE-2024-26308 (snowflakedb/snowflake-jdbc#2538)
    - Add SLF4J bridge from shaded dependencies to `SFLogger` (snowflakedb/snowflake-jdbc#2543)
    - Fixed proxy authentication when connecting to GCP (snowflakedb/snowflake-jdbc#2540)
    - Fixed bug where called-provided schema was ignored in getStreams() (snowflakedb/snowflake-jdbc#2546)
    - Fixed S3 error handling manifested with `NullPointerException` (snowflakedb/snowflake-jdbc#2550)

- v4.0.1
    - Add /etc/os-release data to Minicore telemetry (snowflakedb/snowflake-jdbc#2470)
    - Fix incorrect encryption algorithm chosen when a file was put to S3 with client_encryption_key_size account parameter set to 256 (snowflakedb/snowflake-jdbc#2472) 
    - Fixed fat jar with S3 iteration, the problem of not finding class `software.amazon.awssdk.transfer.s3.internal.ApplyUserAgentInterceptor` (snowflakedb/snowflake-jdbc#2474).
    - Removed Conscrypt from shading to prevent `failed to find class org/conscrypt/CryptoUpcalls` native error (snowflakedb/snowflake-jdbc#2474).
    - Update BouncyCastle dependencies to fix CVE-2025-8916 CVE-2025-8885 (snowflakedb/snowflake-jdbc#2479)
    - Fix external browser authentication after changing enum name. Manifested with `Invalid connection URL: Invalid SSOUrl found` error (snowflakedb/snowflake-jdbc#2475).
    - Rolled back external browser authenticator name to `externalbrowser` (snowflakedb/snowflake-jdbc#2475).

__Due to some underlying issues, Snowflake recommends that AWS and Azure customers do not upgrade to this version if you use PUT or GET queries. Instead, Snowflake recommends that you upgrade directly to version 4.0.1. If you have already upgraded to this version, please upgrade to version 4.0.1 as soon as possible.__
- v4.0.0
    - Bumped netty to 4.1.130.Final to address CVE-2025-67735 (snowflakedb/snowflake-jdbc#2447)
    - Fix OCSP HTTP client cache to honor per-connection proxy settings (snowflakedb/snowflake-jdbc#2449)
    - Mask secrets in exception logging (snowflakedb/snowflake-jdbc#2457)
    - Fix NPE when sending in-band telemetry without HTTP response (snowflakedb/snowflake-jdbc#2460)
    - Migrate from AWS SDK v1 to AWS SDK v2 (snowflakedb/snowflake-jdbc#2385 snowflakedb/snowflake-jdbc#2393)
    - Return column_size value in database metadata commands as in JDBC spec (snowflakedb/snowflake-jdbc#2418)
    - Migrate Azure storage from v5 to v12 (snowflakedb/snowflake-jdbc#2417)
    - Enable bundled BouncyCastle for private key decryption by default (snowflakedb/snowflake-jdbc#2452)
    - Rename BouncyCastle JVM property from net.snowflake.jdbc.enableBouncyCastle to net.snowflake.jdbc.useBundledBouncyCastleForPrivateKeyDecryption (snowflakedb/snowflake-jdbc#2452).
    - Major public API restructuring: move all public APIs to net.snowflake.client.api.* package hierarchy (snowflakedb/snowflake-jdbc#2403):
      - Add new unified QueryStatus class in public API that replaces the deprecated QueryStatus enum and QueryStatusV2 class.
      - Add new public API interfaces for stream upload/download configuration (DownloadStreamConfig, UploadStreamConfig).
      - Add SnowflakeDatabaseMetaData interface to public API for database metadata operations.
      - Add SnowflakeAsyncResultSet interface to public API for async query operations.
      - Add SnowflakeResultSetSerializable interface to public API.
      - Deprecate net.snowflake.client.jdbc.SnowflakeDriver in favor of new net.snowflake.client.api.driver.SnowflakeDriver.
      - Move internal classes to net.snowflake.client.internal.* package hierarchy.
      - Removed deprecated com.snowflake.client.jdbc.SnowflakeDriver class.
      - Removed deprecated QueryStatus enum from net.snowflake.client.core package.
      - Removed deprecated QueryStatusV2 class from net.snowflake.client.jdbc package.
      - Removed deprecated SnowflakeType enum from net.snowflake.client.jdbc package.

- v3.28.0
    - Ability to choose connection configuration in auto configuration file by a parameter in JDBC url. (snowflakedb/snowflake-jdbc#2369)
    - Bumped grpc-java to 1.77.0 to address CVE-2025-58057 from transient dep (snowflakedb/snowflake-jdbc#2415)
    - Fix Connection and socket timeout are now propagated to HTTP client (snowflakedb/snowflake-jdbc#2394).
    - Fix Azure 503 retries and configure it with the putGetMaxRetries parameter (snowflakedb/snowflake-jdbc#2422).
    - Improved retries for SSLHandshakeException errors caused by transient EOFException (snowflakedb/snowflake-jdbc#2423)
    - Introduced shared library([source code](https://github.com/snowflakedb/universal-driver/tree/main/sf_mini_core)) for extended telemetry to identify and prepare testing platform for native rust extensions (snowflakedb/snowflake-jdbc#2430)
    - Bumped netty to 4.1.128.Final to address CVE-2025-59419 (snowflakedb/snowflake-jdbc#2389)

- v3.27.1
    - Added platform detection on login to set PLATFORM metric in CLIENT_ENVIRONMENT (snowflakedb/snowflake-jdbc#2351)
    - Disable DatabaseMetaDataLatestIT::testUseConnectionCtx test (snowflakedb/snowflake-jdbc#2367)
    - Fix IT tests to construct OAuth scopes correctly (snowflakedb/snowflake-jdbc#2366)
    - Fix exponential backoff retry time for non-auth requests (snowflakedb/snowflake-jdbc#2370)
    - Upgrade aws-sdk to 1.12.792 and add STS dependency (snowflakedb/snowflake-jdbc#2361)
    - Add rockylinux9 CI tests as part of RHEL 9 support (snowflakedb/snowflake-jdbc#2368)
    - Bumped grpc-java to 1.76.0 to address CVE-2025-58056 from transient dep (snowflakedb/snowflake-jdbc#2371)
    - Added `workloadIdentityImpersonationPath` config option for `authenticator=WORKLOAD_IDENTITY` allowing workloads to authenticate as a different identity through transitive service account impersonation (snowflakedb/snowflake-jdbc#2348)
    - Added support for authentication as a different identity through transitive IAM role impersonation for AWS (snowflakedb/snowflake-jdbc#2364)
    - Add AWS identity detection with ARN validation (snowflakedb/snowflake-jdbc#2379)
  
- v3.27.0
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs (snowflakedb/snowflake-jdbc#2340).
    - Added HTTP 307 & 308 retries in case of internal IP redirects (snowflakedb/snowflake-jdbc#2344)
    - Make PAT creation return `ResultSet` when using `execute` method (snowflakedb/snowflake-jdbc#2343)
    - Renamed CRL_REVOCATION_CHECK_MODE to CERT_REVOCATION_CHECK_MODE in CLIENT_ENVIRONMENT metrics (snowflakedb/snowflake-jdbc#2349)
    - Test coverage for multistatement jdbc (snowflakedb/snowflake-jdbc#2318).
    - Fixed permission check for .toml config file (snowflakedb/snowflake-jdbc#2270).
    - Bumped netty to 4.1.127.Final to address CVE-2025-58056 and  CVE-2025-58057 (snowflakedb/snowflake-jdbc#2354)
    - Add support for x-snowflake-session sticky HTTP session header returned by Snowflake (snowflakedb/snowflake-jdbc#2357)
    - Added support for Interval Year-Month and Day-Time types in JDBC (snowflakedb/snowflake-jdbc#2345).
    - Added support for Decfloat types in JDBC (snowflakedb/snowflake-jdbc#2329, snowflakedb/snowflake-jdbc#2332).
    - Fixed pattern search for file when QUOTED_IDENTIFIERS_IGNORE_CASE enabled (snowflakedb/snowflake-jdbc#2333)
    - Added support for CRL (certificate revocation list) (snowflakedb/snowflake-jdbc#2287).

<!-- Retrospective entries below were backfilled from git history and highlight major features and critical fixes only. -->

- v3.26.1
  - Fixed NullPointerException when MFA is enabled during native Okta authentication (snowflakedb/snowflake-jdbc#1887)
  - Bumped netty to 4.1.124.Final to address CVE-2025-55163 (snowflakedb/snowflake-jdbc#2323)
  - Fixed HTTP client cache retaining CloseableHttpClient instances after connection pool shutdown (snowflakedb/snowflake-jdbc#2224)
  - Added TLSv1.3 support (snowflakedb/snowflake-jdbc#2313)

- v3.26.0
  - Fix binding array of timestamps (snowflakedb/snowflake-jdbc#2289)
  - Improved Workload Identity Federation error messages (snowflakedb/snowflake-jdbc#2298)
  - Removed experimental feature flag requirement for Workload Identity Federation (snowflakedb/snowflake-jdbc#2275)
  - Add time/date/timestamp array bindings (snowflakedb/snowflake-jdbc#2285)

- v3.25.1
  - Added `enablePatternSearch` connection property for pattern matching in DatabaseMetaData SHOW commands (snowflakedb/snowflake-jdbc#2265)
  - Add retries for protocol_version error during TLS negotiation (snowflakedb/snowflake-jdbc#2246)
  - Added AWS SDK-based GCS access strategy for file transfers (snowflakedb/snowflake-jdbc#2228)

- v3.25.0
  - Fixed access token expiration handling for legacy OAuth flow (snowflakedb/snowflake-jdbc#2226)
  - Added sovereign cloud support and removed obsolete issuer checks for Workload Identity Federation (snowflakedb/snowflake-jdbc#2230)
  - Fix "signature" formatting in "DESC ROW ACCESS POLICY" (snowflakedb/snowflake-jdbc#2221)

- v3.24.2
  - Fixed trust manager algorithm name selection (snowflakedb/snowflake-jdbc#2202)

- v3.24.1
  - Fixed OCSP certificate chain traversal vulnerability by stopping when root certificate is found (snowflakedb/snowflake-jdbc#2189)
  - Fixed AWS Workload Identity Federation request signature generation (snowflakedb/snowflake-jdbc#2191)
  - Add support for HTTP header customizers (snowflakedb/snowflake-jdbc#2178)

- v3.24.0
  - Move OCSP setup earlier in session initialization (snowflakedb/snowflake-jdbc#2167)
  - Fixed TIMESTAMP_LTZ binding breaking other date/time type bindings (snowflakedb/snowflake-jdbc#2156)
  - Fixed OCSP cache server URL when using an HTTP proxy (snowflakedb/snowflake-jdbc#2149)
  - Added client-side opt-in for OAuth refresh token rotation (snowflakedb/snowflake-jdbc#2166)

- v3.23.2
  - Fixed NullPointerException when credential cache folder is inaccessible (snowflakedb/snowflake-jdbc#2108)
  - Added initial DPoP (Demonstrating Proof-of-Possession) support (snowflakedb/snowflake-jdbc#2127)

- v3.23.1
  - Added retries for Okta native SSO authentication to prevent HTTP retry storms (snowflakedb/snowflake-jdbc#2064)
  - Fixed credential cache file creation on Windows (snowflakedb/snowflake-jdbc#2115)

- v3.23.0
  - Added PAT, Native OAuth, and secure token cache support (snowflakedb/snowflake-jdbc#1978)
  - Gather threadExecutor callables and call Future.get() to prevent silent fails (snowflakedb/snowflake-jdbc#2035)
  - Improve exception message when getting query metadata (snowflakedb/snowflake-jdbc#2059)
  - Let query timeout be server side or client side, not both (snowflakedb/snowflake-jdbc#2084)

- v3.22.1
  - Maintenance release with bug fixes and dependency updates.

- v3.22.0
  - Fixed external browser authentication on Windows (snowflakedb/snowflake-jdbc#2053)
  - Fix validity checks in arrow struct vectors (snowflakedb/snowflake-jdbc#2022)
  - Narrow toString calculation to structured types only (snowflakedb/snowflake-jdbc#2007)
  - Implement setQueryTimeout for async queries (snowflakedb/snowflake-jdbc#1958)

- v3.21.1
  - Maintenance release with bug fixes and dependency updates.

- v3.21.0
  - Fixed structured types string conversion performance regression (snowflakedb/snowflake-jdbc#1991)
  - Adding flag to skip token file permission verification (snowflakedb/snowflake-jdbc#1959)
  - Fixed native library relocation in shaded JAR (snowflakedb/snowflake-jdbc#1927)
  - Bumped netty to address CVE-2024-47535 (snowflakedb/snowflake-jdbc#1962)

- v3.20.1
  - Maintenance release with bug fixes and dependency updates.

- v3.20.0
  - Fixed encryption key size validation for GCP and Azure file transfers (snowflakedb/snowflake-jdbc#1946)

- v3.19.1
  - Fixed case-sensitive custom cloud storage header metadata handling (snowflakedb/snowflake-jdbc#1919)
  - Arrow-shared needs logging changes ported from previous version (snowflakedb/snowflake-jdbc#1922)
  - Unify structured types string representation (snowflakedb/snowflake-jdbc#1882)

- v3.19.0
  - Add connection property for setting browser timeout value (snowflakedb/snowflake-jdbc#1824)
  - Support private key base64 in connection parameters (snowflakedb/snowflake-jdbc#1847)
  - Upgrade to Arrow 17.0.0 in snowflake-jdbc (snowflakedb/snowflake-jdbc#1854)
  - Continuing SNOW-1020043: expose timeouts (DEFAULT_CONNECTION_TIMEOUT and DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT) in JDBC as Connection string and/or datasource property (snowflakedb/snowflake-jdbc#1865)

- v3.18.1
  - Maintenance release with bug fixes and dependency updates.

- v3.18.0
  - Add check of user permission for token file (snowflakedb/snowflake-jdbc#1835)
  - Fix conversion of OBJECT column nested fields metadata (snowflakedb/snowflake-jdbc#1831)
  - Add rich results metadata support for Coldbrew/Snowsight integration (snowflakedb/snowflake-jdbc#1840)
  - Add Properties setters in SnowflakeBasicDataSource (snowflakedb/snowflake-jdbc#1800)

- v3.17.1
  - Maintenance release with bug fixes and dependency updates.

- v3.17.0
  - Support for host when use file configuration (snowflakedb/snowflake-jdbc#1809)
  - Add JDBC Connectivity Diagnostics mode (snowflakedb/snowflake-jdbc#1789)
  - Return consistent timestamps_ltz between JSON and ARROW result sets (snowflakedb/snowflake-jdbc#1792)

- v3.16.1
  - Use S3 regional url domain base on region name (snowflakedb/snowflake-jdbc#1768)
  - SNOW-1432770 Update DatabaseMetaDataInternalIT testGetTables to work with adding account_usage_snowhouse_local_setup_import.sql to make setup (snowflakedb/snowflake-jdbc#1760)
  - Add parameter to disable SAML URL check (snowflakedb/snowflake-jdbc#1748)

- v3.16.0
  - Fix native okta retry logic (snowflakedb/snowflake-jdbc#1701)
  - Structured types backward compatibility for getObject method (snowflakedb/snowflake-jdbc#1740)
  - Write and bindings structured types (snowflakedb/snowflake-jdbc#1727)
  - Set max retries for get query metadata (snowflakedb/snowflake-jdbc#1732)

- v3.15.1
  - Handle nulls in structured types (snowflakedb/snowflake-jdbc#1695)
  - Add checks for structured types for getMap, getObject and getArray (snowflakedb/snowflake-jdbc#1694)

- v3.15.0
  - Add new connection property enablePatternSearch to flag if pattern searches for some DatabaseMetaData methods are allowed (snowflakedb/snowflake-jdbc#1639)
  - Fix deprecation links (snowflakedb/snowflake-jdbc#1640)
  - Fix parsing large string values with jackson databind > 2.15 (snowflakedb/snowflake-jdbc#1613)
  - Configure connection and socket timeout (snowflakedb/snowflake-jdbc#1628)

- v3.14.5
  - Replace arrow-shared jars with those that have the fix for AIX (snowflakedb/snowflake-jdbc#1605)
  - Implement toString() in SnowflakePreparedStatementV1 (snowflakedb/snowflake-jdbc#1604)
  - Implement queryStatusV2 (snowflakedb/snowflake-jdbc#1579)

- v3.14.4
  - Snowflake JDBC drivers fails on JDK 21 with Arrow (snowflakedb/snowflake-jdbc#1529)
  - Add codecov.yml to count partial hit (snowflakedb/snowflake-jdbc#1562)
  - Fix testPreparedStatement, fix resource management in LogicalConnectionLatestIT (snowflakedb/snowflake-jdbc#1569)

- v3.14.3
  - Add JDBC_JAR_NAME to client environment (snowflakedb/snowflake-jdbc#1547)
  - Add createSessionlessTelemetry(CloseableHttpClient, String) method (snowflakedb/snowflake-jdbc#1527)
  - : Added connection property to disable PUT/GET command (snowflakedb/snowflake-jdbc#1523)

- v3.14.2
  - Add error code 390400 to testHttpsLoginTimeoutWithOutSSL (snowflakedb/snowflake-jdbc#1516)

- v3.14.1
  - Add path to private link OCSP retry url (snowflakedb/snowflake-jdbc#1495)
  - Refactor to support async in stored proc (snowflakedb/snowflake-jdbc#1324)
  - Add additionalHeader support (snowflakedb/snowflake-jdbc#1490)

- v3.14.0
  - [Snyk] Fix for 1 vulnerabilities (snowflakedb/snowflake-jdbc#1429)
  - Log scrubbed URL for chunk download timeout (snowflakedb/snowflake-jdbc#1456)
  - Fix issue with consecutive batch array binding not working (snowflakedb/snowflake-jdbc#1439)
  - Disable max retries in chunk downloader if maxHttpRetries is 0 (snowflakedb/snowflake-jdbc#1488)

- v3.13.33
  - Alter parameter to upload using presigned url (snowflakedb/snowflake-jdbc#1412)
  - Java best practice changes for streaming sdk (snowflakedb/snowflake-jdbc#1382)

- v3.13.32
  - Maintenance release with bug fixes and dependency updates.

- v3.13.31
  - [SNOW-804206] 2. Add Custom snowflake log levels (snowflakedb/snowflake-jdbc#1378)
  - [SNOW-799391] Add troubleshoot msg for ssl errors (snowflakedb/snowflake-jdbc#1369)

- v3.13.30
  - Fix memory leak by checking isClosed() before adding resultset to openResultSets (snowflakedb/snowflake-jdbc#1303)
  - Moved PrintWriter to a try-with-resources block to avoid a resource leak (snowflakedb/snowflake-jdbc#1352)
  - Fix auth timeout issue on chunk downloader requests (snowflakedb/snowflake-jdbc#1355)

- v3.13.29
  - Fix: driver returns empty column type name for stored procedure metadata when new statement type call is enabled (snowflakedb/snowflake-jdbc#1281)
  - Added URLValidator and URLEncoder (snowflakedb/snowflake-jdbc#1297)

- v3.13.28
  - Support resultset columns in getProcedureColumns() and getFunctionColumns() (snowflakedb/snowflake-jdbc#1242)
  - Add support for customizing user agent in JDBC request header (snowflakedb/snowflake-jdbc#1215)
  - Support GEOMETRY type (snowflakedb/snowflake-jdbc#1237)

- v3.13.27
  - Fix SNOW-724666: Retry http status code 429 (snowflakedb/snowflake-jdbc#1220)
  - Fix race condition in SnowflakeDatabaseMetaData.java show command (snowflakedb/snowflake-jdbc#1212)

- v3.13.26
  - Add classpath to manifest file (snowflakedb/snowflake-jdbc#1206)
  - Upgrade arrow to version 10.0.1 with new ssl version (snowflakedb/snowflake-jdbc#1202)
  - #550 fix the special case for procedure parameters reading (snowflakedb/snowflake-jdbc#1130)

- v3.13.25
  - Handle Session is Null when returning result type (snowflakedb/snowflake-jdbc#1185)
  - Add enable timestamp with timezone parameter (snowflakedb/snowflake-jdbc#1173)
  - Add warnings for errors while parsing a SnowflakeConnectString (snowflakedb/snowflake-jdbc#1166)

- v3.13.24
  - S3 FIPS support fix. (snowflakedb/snowflake-jdbc#1136)
  - [Snyk] Fix for 3 vulnerabilities (snowflakedb/snowflake-jdbc#1156)
  - Add repo_meta.yaml file to repositories (snowflakedb/snowflake-jdbc#618)

- v3.13.23
  - [Snyk] Fix for 10 vulnerabilities (snowflakedb/snowflake-jdbc#1129)
  - [Snyk] Fix for 11 vulnerabilities (snowflakedb/snowflake-jdbc#1140)
  - Set default logger level for arrow project to SEVERE to avoid unwanted INFO messages (snowflakedb/snowflake-jdbc#1133)

- v3.13.22
  - Fix for non array binding
  - Add support for new OKTA OIE to JDBC Driver (snowflakedb/snowflake-jdbc#1122)
  - Implement getSQLStateType()

- v3.13.21
  - Fix JDBC chunk downloader missing data issue (snowflakedb/snowflake-jdbc#1047)
  - Clear batchOfVectors list when freeing chunk data
  - Add method to list streams

- v3.13.20
  - Added fix for failing new datatype (snowflakedb/snowflake-jdbc#1033)
  - Trying with new file (snowflakedb/snowflake-jdbc#1036)
  - Map timestamp_tz to java.sql.Type TIMESTAMP_WITH_TIMEZONE

- v3.13.19
  - Merge pull request #906 from snowflakedb/address-security-vulnerabilities
  - Bug fix for stage binding with TIMESTAMP_INPUT_FORMAT
  - Add pattern matching for getPrimaryKeys() and getForeignKeys

- v3.13.18
  - Upgrade arrow and jackson versions to fix vulnerabilities (snowflakedb/snowflake-jdbc#894)
  - Don't append retryCount to the scoped URL for chunk downloading (snowflakedb/snowflake-jdbc#907)
  - Wrong retry 403 logic (snowflakedb/snowflake-jdbc#916)

- v3.13.17
  - Merge pull request #890 from snowflakedb/http-proxy-parameters-are-not-recoginzed-as-expected
  - Merge pull request #886 from snowflakedb/SNOW-348660-Add-getters-and-setters-for-timezone

- v3.13.16
  - Fix issue where database name is double quoted in catalog function (snowflakedb/snowflake-jdbc#827)
  - SNOW 500881 JWT expiration issue (snowflakedb/snowflake-jdbc#715)

- v3.13.15
  - Regionless URL support in JDBC Driver (snowflakedb/snowflake-jdbc#703)
  - Support sessionless client telemetry [JDBC side] (snowflakedb/snowflake-jdbc#609)
  - Fix ChunkDownloader hanging issue (snowflakedb/snowflake-jdbc#690)

- v3.13.14
  - Adding streaming ingest related metadata for streaming ingest billing (snowflakedb/snowflake-jdbc#672)
  - SNOW-494684 Fix DatabaseMetadata bugs -double quotes and wildcards (snowflakedb/snowflake-jdbc#681)

- v3.13.13
  - Fix for S3 Regional URL not being updated in stageInfo (snowflakedb/snowflake-jdbc#671)
  - Fix account name breakage and update host url (snowflakedb/snowflake-jdbc#670)

- v3.13.12
  - JVM http proxy properties do not work for PUT/GET (snowflakedb/snowflake-jdbc#652)
  - Fix diffs

- v3.13.11
  - Fix bug where time(3) objects weren't honoring USE_SESSION_TIMEZONE when called with getTimestamp() in Arrow format (snowflakedb/snowflake-jdbc#619)

- v3.13.10
  - Make chunk downloader max retry configurable with a server side parameter (snowflakedb/snowflake-jdbc#610)
  - Fix version
  - Fix exception

- v3.13.9
  - Add API to get query IDs for multiple statements (snowflakedb/snowflake-jdbc#590)
  - Support SFAsyncResultSet.getResultSetSerializables(Long) (snowflakedb/snowflake-jdbc#586)

- v3.13.8
  - Add more log to verify investigation (snowflakedb/snowflake-jdbc#567)
  - Add log to an azure exception case (snowflakedb/snowflake-jdbc#566)

- v3.13.7
  - Add questions to PR template for external pull request creators (snowflakedb/snowflake-jdbc#546)
  - Run semgrep in temp dir and use new baseline ref (snowflakedb/snowflake-jdbc#536)
  - Add shutdown() to the ExecutorService in sendTelemetryClient

- v3.13.6
  - Put hook into Telemetry instead
  - Add semgrep merge gate to jdbc (snowflakedb/snowflake-jdbc#527)

- v3.13.5
  - Http client map has unique values for different proxy configurations (snowflakedb/snowflake-jdbc#524)
  - Added jira auto closure

- v3.13.4
  - Do not try to renew cloud storage tokens if the session is null (snowflakedb/snowflake-jdbc#514)
  - Merge pull request #509 from snowflakedb/mnaides-SNOW-348866-sessionless-put
  - Support unencrypted sessionless upload

- v3.13.3
  - Release chunk memory if download fails
  - Fixed nullptrexception in arrow getBigDecimal function (snowflakedb/snowflake-jdbc#478)
  - Took out json-smart whitesource vulnerability (snowflakedb/snowflake-jdbc#500)

- v3.13.2
  - Fix metadata schema scoping bug (snowflakedb/snowflake-jdbc#465)
  - Add connection context, session schema, and session database to metadata telemetry message (snowflakedb/snowflake-jdbc#463)
  - Fix timestamp_tz bug for ResultSet.getTimestamp()/getTime()/getDate() with JDBC_USE_SESSION_TIMEZONE parameter (snowflakedb/snowflake-jdbc#468)

- v3.13.1
  - Fix session token expiry and heartbeat frequency settings (snowflakedb/snowflake-jdbc#451)
  - Clean up memory chunks after thread is interrupted (snowflakedb/snowflake-jdbc#456)

- v3.13.0
  - Fix Azure proxy connection (snowflakedb/snowflake-jdbc#448)
  - Fix telemetry message to show escaped characters (snowflakedb/snowflake-jdbc#449)
  - Add comment
  - Adjustable file threshold for put in jdbc (snowflakedb/snowflake-jdbc#423)

- v3.12.17
  - Change bulk array binds to be uploaded with streaming PUT instead of local file creation + PUT (snowflakedb/snowflake-jdbc#418)
  - Remove the reference to the change that adds support for MFA token caching
  - Use session param to display time/date/timestamps in session timezone (snowflakedb/snowflake-jdbc#397)

- v3.12.16
  - Reformat SnowflakeSQLLoggedExceptions in-band telemetry messages and add telemetry for SqlFeatureNotSupportedExceptions (snowflakedb/snowflake-jdbc#389)
  - Updating whitesource vulnerabilities (snowflakedb/snowflake-jdbc#393)

- v3.12.15
  - Merge pull request #386 from snowflakedb/azhan-rm-enable-fix-63095
  - Added new codec version SNOW-223232
  - Update whitesource vulnerabilities for v3.12.15

- v3.12.14
  - Fix empty batch when using array bind
  - Add back close()
  - Fix jira comments (snowflakedb/snowflake-jdbc#378)

- v3.12.13
  - Google oauth client vulnerability #366
  - Increase timeout for telemetry OOB (snowflakedb/snowflake-jdbc#342)
  - Added few release notes

- v3.12.12
  - Fixed a bug that caused session to be null (snowflakedb/snowflake-jdbc#330)
  - Enhance SnowflakeResultSetSerializable.getResultSet() to support private link env.
  - Added config files for secret scanning JDBC (snowflakedb/snowflake-jdbc#329)

- v3.12.11
  - Fix displaying GEOGRAPHY type in JDBC driver
  - Add telemetry to collect metrics on DatabaseMetadata Get functions (snowflakedb/snowflake-jdbc#308)
  - Merge pull request #287 from snowflakedb/wshangguan-SNOW-176125-add-telemetry-to-debug

- v3.12.10
  - Use latest-commit for target JDBC driver to download (snowflakedb/snowflake-jdbc#292)
  - Add Semgrep Check
  - Send telemetry data for SnowflakeSQLLoggedExceptions (snowflakedb/snowflake-jdbc#282)

- v3.12.9
  - Add log analyze step to precommit
  - Add CLIENT_DRIVER_NAME
  - Add new space line

- v3.12.8
  - Add client connection parameters to clientEnv map to be stored in Snowhouse (snowflakedb/snowflake-jdbc#257)
  - Fixed snowflake-common version (#266) (snowflakedb/snowflake-jdbc#266)
  - CASP-828 fix wss script for master branch (snowflakedb/snowflake-jdbc#259)

- v3.12.7
  - Maintenance release with bug fixes and dependency updates.

- v3.12.6
  - Add wss.sh to JDBC (snowflakedb/snowflake-jdbc#249)
  - Making new parameter false by default
  - Speeding up async ResultSet.next() performance by adding exponential backoff

- v3.12.5
  - Fix ConcurrentModification exception when terminating chunk downloader
  - Added 1 release note for jdbc 3.12.5
  - Add getMetadata() function for async resultsets, add error messages for failed query statuses, clean up nullptr exceptions

- v3.12.4
  - Fix DLS error by adding session parameter to allow Timestamp_NTZ to be stored always in UTC timezone
  - Add failure count and last known error functions to telemetry service
  - Add more info to user-agent header

- v3.12.3
  - JDBC: fix memory leak caused by telemetry service
  - CVE-2020-8840 CVE-2019-20330 Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.10.3 or later
  - Add tracing option snowflakedatasource

- v3.12.2
  - Enhance JDBC File Transfer API to support proxy
  - Enable client formatting check in non-blocking mode
  - Enhance JDBC to provide new API for spark connector GCP

- v3.12.1
  - Changed PUT threashold for multi-part upload to 64MB
  - Add snowflake JDBC fips
  - Add actions

- v3.12.0
  - Add new statement types for LIST, GET, PUT, RM
  - Add parameter CLIENT_METADATA_USE_SESSION_DATABASE
  - Added new functions to getStringFunctions, getNumericFunctions

- v3.11.1
  - Add interface functions to retrieve statistic metadata from SnowflakeResultSetSerializable
  - Added logging for PreparedStatement binding parameters.
  - Adding logging info for key pair authentication

- v3.11.0
  - Fix NullPointerException result from no proxy port
  - Fix fix jdbc bug for closing all chunk's memory usage
  - Fix arrow data not allocating more than 2GB
  - Make sure memoryLimit can avoid OOM for Arrow buffer

- v3.10.3
  - Update OCSP Cache Expiration to 5 days
  - Patched secret detector for chunk url

- v3.10.2
  - : fix SnowflakeDriverIT::testSnow14774 and SnowflakeDriverIT::testSnow31104 issues when turn on Arrow
  - : Fixed CVE-2019-16942 and CVE-2019-17195

- v3.10.1
  - Implement retry for chunk downloader to resolve customer issue
  - Fixed CVE-2019-16335, CVE-2019-14540
  - Added getSessionID() function to cache session ID in connection class

- v3.10.0
  - Fix CVE-2019-16335, CVE-2019-14540: com.fasterxml.jackson.core:jackson-databind
  - Add getBytes cross testing, JSON and Arrow formats match
  - Fix Special chars can now be used as table names, function names

- v3.9.2
  - Fix JDBC Arrow Result Debug: getBoolean
  - Add an example using downloadStream() to complement the existing example that uses uploadStream().

- v3.9.1
  - Fix OCSP check hang while locking cache
  - Fix no OCSP URL case

- v3.9.0
  - Fix arrow format issue
  - Fix fails to decode the value for datatype time(0) in arrow
  - Fix NPE in OOB telemetry
  - Add Statement.getLargeCount, executeLargeUpdate

- v3.8.8
  - Enable client side sorting for Arrow
  - : support all data types for Arrow format in XP and JDBC
  - Fix timestamp epoch/fraction bug when timestmap is negative but epoch is zero

- v3.8.7
  - Support DATE, TIME, TIMESTAMP data types in JDBC for arrow result format
  - Increase the adjust steps in conservative memory usage mode to better utilize result cache

- v3.8.6
  - Add oktausername parameter support to OKTA authenticator
  - Support DATE, TIME, TIMESTAMP data types in JDBC for arrow result format

- v3.8.5
  - Make sure ctx_tags are lowercase in telemetry events
  - Proxy parameter support for PUT to S3. No Azure support is included
  - Support DATE, TIME, TIMESTAMP data types in JDBC for arrow result format

- v3.8.4
  - Support DATE, TIME, TIMESTAMP data types in JDBC for arrow result format

- v3.8.3
  - OCSP Cache Server URL Update for Private Link Customers

- v3.8.2
  - Fix OOB telemetry endpoint regex which caused JDBC ignores sending prod events
  - Filter out empty payload of the OOB telemetry messages
  - Fixed wrong result set when the cache buffer is reused

- v3.8.1
  - Use equals method instead of double equal signs even for intern object in OCSP check
  - Ignore session gone error when closing

- v3.8.0
  - OCSP Softfail Support
  - Add OCSP event to the out of band telemetry service in JDBC
  - Changed c3p0 version to fix security vulnerability
  - Implement DatabaseMetaData.getFunctionColumns

- v3.7.2
  - User can change heartbeat frequency (fixed)
  - Add getQueryId method SnowflakeStatementV1 and SnowflakeResultSetV1
  - Added CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY

- v3.7.1
  - Implement ResultJsonParserV2 to reduce memory usage by half and also improve parsing speed
  - Partial support of PreparedStatement.getParameterMetaData
  - Add login input to log

- v3.7.0
  - Added detail error messages on OOM when fetching large result set
  - Fixed JDBC code formats

- v3.6.28
  - Verify validity of cert attached to OCSP Responses
  - Fixed memory leak when thread concurrency is more than 20

- v3.6.27
  - Fixed NPE when binding null for Timemstamp
  - Expose the upload from stream API in the doc.

- v3.6.26
  - Fixed binding null with data type raising error. SNOW-64977: Better error message in case binding datatype is not consistent.
  - Added changelog for 3.6.24 and 3.6.25
  - Implement PooledConnection interface and ConnectionPoolDataSource interface

- v3.6.25
  - Explicitly allows all proxy data to be fed via the JDBC connection string
  - Add support to boomi bouncy castle workaround
  - Initialize logger onlhy if tracing parameter is set, otherwise JDBC driver will let application code to enable logging. Removed slf4j auto detection.

- v3.6.24
  - Fixed support link
  - Upgrade .jackson.core:jackson-databind to 2.9.8 to fix security vulnerability
  - Add enum REJECTED_RECORD in class LoadingError

- v3.6.23
  - Upgraded tika-core to 1.20 to fix vulnerability
  - Add more null checks in next() and close() for DatabaseMetaData result sets
  - Fixed invalid retry OCSP URL for privatelink

- v3.6.21
  - Use YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM for timestamp array binding

- v3.6.20
  - SNOW-61209 jdbc client memory setting debug

- v3.6.19
  - Maintenance release with bug fixes and dependency updates.

- v3.6.18
  - Piecemeal fetch support (ResultSet.getCharacterStream) in JDBC

- v3.6.17
  - Fix the bug that parallel was not honored for GET command.
  - Add JVM properties for CLIENT_PREFETCH_THREADS, CLIENT_MEMORY_LIMIT and CLIENT_RESULT_CHUNK_SIZE.
  - SCR-14911 support CLOB in JDBC

- v3.6.16
  - CLIENT_MEMORY_LIMIT governs the total memory used for chunk downloader per process not per query (patch: fixed a memory release bug and make sure no hanging)
  - CLIENT_MEMORY_LIMIT governs the total memory used for chunk downloader per process not per query
  - CVE-2018-11761 fix for JDBC driver.

- v3.6.15
  - Maintenance release with bug fixes and dependency updates.

- v3.6.14
  - Add cach id token consent page support in JDBC.
  - Add request_guid for JDBC
  - Added SERVICE_NAME support to JDBC.

- v3.6.13
  - Upgraded dependency to fix known Vulnerabilities.
  - Added compressFileByPut, compressDataBeforePut, compressLevel to Loader API. No change in PUT and GET compression ration.
  - Enable multi statement in same session

- v3.6.12
  - Maintenance release with bug fixes and dependency updates.

- v3.6.11
  - Support POST request to receive the token from the browsers. Fixed Windows compile error.
  - Fixed the URL query parser to get multiple values
  - Add badge to README.rst

- v3.6.10
  - Add jitter to JDBC request retry
  - Heartbeat to refresh token hourly
  - Return empty result set for function columns metadata

- v3.6.9
  - Support the use of JVM launch options to configure file cache directories
  - Add client support for disabling SOCKS proxy for JDBC traffic

- v3.6.8
  - Add retryCount and clientStartTime to query-request requests on JDBC

- v3.6.7
  - Split OCSP code and file cache manager.
  - Added onError parameter for COPY in Loader API.

- v3.6.6
  - Retry OCSP check doesn't occur if validity check fails.

- v3.6.5
  - Add support for timestamps + dates in stage array bind
  - Add support to GS and JDBC for bulk array binds uploaded to stage
  - Added a support link to the doc

- v3.6.4
  - Add a parameter to control telemetry service
  - Add setAuthenticator to SnowflakeBasicDataSource
  - Fixed 50MB calculation.

- v3.6.3
  - Add getter to expose stage info to spark connector
  - Fix update count when 0 copy file is processed.

- v3.6.2
  - Add url checker
  - Add client-side telemetry metrics
  - Adding copyEmptyFieldAsEmpty option to the loader API.

- v3.6.1
  - Updated JDBC to support OCSP dynamic cache server.

- v3.6.0
  - Enable OCSP Response Cache server for JDBC by default.
  - Merge pull request #14 from tildedave/tildedave/add-java-sql-driver-for-drivermanager-auto-discovery
  - Add java.sql.Driver file

- v3.5.5
  - SF_OCSP_RESPONSE_CACHE_DIR is used to specify the OCSP cache file location.
  - Add APPLICATION as a connection property

- v3.5.4
  - Mitigate OCSP validity issue.
  - Fixed SnowflakeSQLException: Identity provider configuration for the specified authenticator does not match with your Snowflake account configuration (destination URL mismatch).

- v3.5.3
  - Remove common-lang3 dependency from jdbc. Also refactor session property management

- v3.5.2
  - Maintenance release with bug fixes and dependency updates.

- v3.5.0
  - OCSP revication check for JDBC.
  - Get SecureRandom instances by not specifing provider name since jdbc could be run under ibm jdk. Also remove some unused import
  - Fixes a bug in the JDBC driver that doesn't concatenate paths correctly if the path doesn't have a folder component.

- v3.4.3
  - Support key pair based authentication in JDBC

- v3.4.2
  - Support String literal binding for object name

- v3.4.1
  - Maintenance release with bug fixes and dependency updates.

- v3.4.0
  - Fix NumberFormatException when calling setLoginTimeout

- v3.3.3
  - JDBC Support for OAuth
  - Use ContentMD5 for Azure in JDBC PUT
  - Fix the wrong date before 1582 by getTimestamp(). Refactor some of the code.

- v3.3.2
  - JDBC is updated to correctly set Azure blob metadata to specify that they upload their files using AES CBC key encryption
  - SNOW-36308 Replace AWS_ID and AWS_KEY with their newer versions + compiler warning fixes

- v3.3.1
  - Fix the issue that Azure did not skip files with matching digest

- v3.3.0
  - Fix the initial authenticator request in JDBC

- v3.2.7
  - Updated Fed/SSO parameters for JDBC.

- v3.2.6
  - Maintenance release with bug fixes and dependency updates.

- v3.2.5
  - This is the GS and XP changes for internal stage CSE. Internal stages don't use key wrapping like foreign Azure stages. Instead they use the same key encryption protocol that AWS does so the clients don't need to add significant new crypto support for Azure internal encrypted stages.
  - @SNOW-33566 Implement ResultSet.isBeforeFirst(), isLast(), isAfterLast()

- v3.2.4
  - Add parameter to control whether JDBC treat deciaml as int or not when scale is zero
  - Add support for PUT/GET over encrypted stage files in JDBC for Azure
  - Fix the regression that temp stage did not work with Spark Connector

- v3.2.3
  - ADFS browser authentication support for JDBC driver.

- v3.2.2
  - Fixed the regression. TIME to TIMESTAMP conversion must be supported for Informatica V1 connector.

- v3.2.1
  - Support binding java.sql.Time with TIME data type for Loader API.

- v3.2.0
  - Now that aws sdk is upgrade, proxy setting now can be honored by aws sdk
  - When scale is 0, map Snowflake NUMBER type to java BIGINT. Also, better calculation on display size

- v3.1.1
  - Fix the missing statement type in JDBC

- v3.1.0
  - Support batch dml in JDBC
  - Don't generate incident for OOM error in JDBC

- v3.0.21
  - Support API Statement.addBatch(), executeBatch() and clearBatch().

- v3.0.20
  - Added support for ORC file format in PUT command.

- v3.0.19
  - Add implementation to javax.sql.Datasource interface
  - Fixed Date value and validity for '0001-01-01'. Displays incorrect date output.

- v3.0.18
  - NO-SNOW Shorten heartbeat interval

- v3.0.17
  - Add ZTSD and BROTLI to types of compressed files that JDBC recognizes
  - Add getters for Snowflake Stage Temporary S3 Credentials and Encryption Materials
  - Fix the bug that JDBC failed to parse infinite double value

- v3.0.16
  - Fix the bug that wrongly calculated the login retry time. Also some other cleanup

- v3.0.15
  - Fix the NPE issue when binding a null value in JDBC

- v3.0.14
  - Mark result chunk downloader in JDBC as daemon thread
  - Fixed issue where wasNull flag was not set properly

- v3.0.13
  - Add opensourcing jdbc driver to CHANGELOG.rst
