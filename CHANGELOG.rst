**JDBC Driver 3.13.14**

- \| SNOW-532783 | Updating BC FIPS version in public POM 
- \| SNOW-524708 | Adding streaming ingest related metadata for streaming ingest billing
- \| SNOW-475617 SNOW-494684 | Fix DatabaseMetadata bugs : double quotes and wildcards

**JDBC Driver 3.13.13**

- \|SNOW-520660 | Fix for S3 Regional URL not being updated in stageInfo
- \|SNOW-521194 | Fix account name breakage and update host url  

**JDBC Driver 3.13.12**

- \| SNOW-473749 | Fix bug where time(3) objects weren't honoring USE_SESSION_TIMEZONE when called with getTimestamp() in Arrow format 
- \| SNOW-500624 | Fix JVM http proxy properties to work with PUT/GET 
- \| SNOW-513708 | Refactor bind uploader
- \| | Bouncy Castle FIPS update to 1.0.2.1 
- \SNOW-496117: Add test for backend bug fix 

**JDBC Driver 3.13.11**

- \| SNOW-473749 | Fix bug where time(3) objects weren't honoring USE_SESSION_TIMEZONE when called with getTimestamp() in Arrow format 

**JDBC Driver 3.13.10**

- \| SNOW-492055 | Handle uncaught exceptions that can occur in PUT statements
- \| SNOW-477795 | Regard BLOCKED query as running query.
- \| SNOW-458293 | Added parameter JDBC_CHUNK_DOWNLOADER_MAX_RETRY to make chunk downloader max retry configurable

**JDBC Driver 3.13.9**

- \| SNOW-411516 | Enhance JDBC to support SFAsyncResultSet.getResultSetSerializables(Long).
- \| SNOW-366563 | Fix London/Europe daylight savings offset with timestamp_ntz.
- \| SNOW-412040 | Fix ResultSet.getQueryID() so it returns correct query ID for PUT/GET statements.
- \| SNOW-472364 | Add API to get query IDs for multiple statements.
- \| SNOW-441847 | Cancel Prefetch threads to avoid endless ChunkDownloader hanging thread.
- \| SNOW-464020 | Azure iterator exception bug fix.
- \| SNOW-224719 | Allow curly bracket syntax in JDBC CallableStatement.prepareCall().

**JDBC Driver 3.13.8**

- \| SNOW-443760 | Bumped jsoup library from 1.11.3 to 1.14.2
- \| SNOW-148296 | Introduce new connection parameter to enable JDBC https proxying.
- \| SNOW-449297 | Fully implemented Connection.IsValid() function.

**JDBC Driver 3.13.7**

- \| SNOW-394504 | Fix issue with JDBC rejecting accounts with underscores in them.
- \| SNOW-373633 | Fix issue with JDBC not loading the version properly in all cases.

**JDBC Driver 3.13.6**

- \| SNOW-369447 | Make MAX_CONNECTIONS and MAX_CONNECTIONS_PER_ROUTE changeable with jvm parameters. 

**JDBC Driver 3.13.5**

- \| SNOW-363333 | Add ability to use Multiple proxies in the same JVM.
- \| SNOW-364253 | Update  json-smart library from 2.4.2 to 2.4.5 to remove security vulnerability.

**JDBC Driver 3.13.4**

- \| SNOW-330467 | Fixed an issue with the logic for updating the driver parameters.
- \| SNOW-348866 | Added the ability for clients to pass in data from a PUT command for file upload without creating a Snowflake session.

**JDBC Driver 3.13.3**

- \| SNOW-346424 | Add snowflakeClientInfo connection parameter for use with Spark connector.
- \| SNOW-299374 | In case of private link, have JDBC driver invoke different API to get regional storage link for AWS us-east-1 deployments.
- \| SNOW-332390 | Fix misleading function name isRetryableHTTPCode()
- \| SNOW-299137 | Add log line to show JWT token info.
- \| SNOW-259255 | Add ability to set TIMESTAMP_NTZ and TIMESTAMP_LTZ types with PreparedStatement.setObject() function.
- \| SNOW-334974 | Update json-smart library from 2.3.x to 2.4.2 to remove security vulnerability.
- \| SNOW-344455 | Update commons-io library from 2.2 to 2.8.0 to remove security vulnerability.

**JDBC Driver 3.13.2**

- \| SNOW-285542 | Fixed an issue with the ResultSet.getTimestamp()/getTime()/getDate() methods when the JDBC_USE_SESSION_TIMEZONE parameter was set.
- \| SNOW-297092 | Fixed an issue with scoping metadata requests to the schema in the session context.
- \| SNOW-259063 | Downgraded the Apache httpclient version to 4.12.11 to avoid an S3 certificate error regression.

**JDBC Driver 3.13.1**

- \| SNOW-258666 | Changed the driver to free up memory chunks when a thread is interrupted.
- \| SNOW-281822 | Fixed a session token expiry error and made the heartbeat frequency configurable.

**JDBC Driver 3.13.0**

- \| SNOW-209530 | Changed the handling of proxy settings. Proxy parameters in the connection string now override the JVM proxy settings. When connecting to Azure, PUT/GET commands now go through the specified proxy.
- \| SNOW-165204 | Fixed a number overflow exception that resulted from calling getObject() on a SQL BIGINT value.
- \| SNOW-136474 | Changed the default value of the multi-part threshold from 64 to 200, and changed the JDBC Driver to read this parameter from the server.
- \| SNOW-251457 | Changed  the ResultSet.getString() method to return DATE objects in the session time zone (rather than use the offset from the JVM time zone).
- \| SNOW-250222 | Fixed an exception thrown by the COPY INTO command when the JDBC Driver inserts more than INT_MAX records from a single file.
- \| SNOW-255552 | Fixed a null pointer exception in ResultSet.getCharacterStream().
- \| SNOW-180954 | Removed the JNA classes from the Snowflake JDBC Driver JAR file.

**JDBC Driver 3.12.17**

- \| SNOW-174428 | Change bulk array binds to be uploaded with streaming PUT instead of local file creation + PUT.
- \| SNOW-213443 | Add configurable TimeToLive command line parameter
- \| SNOW-257198 | Updated google guava library from 28.0 to 30.0 to address vulnerability.

**JDBC Driver 3.12.16**

- \| SNOW-206907 | Added support for downscoping GCS credentials (which can be used instead of presigned URLs).

**JDBC Driver 3.12.15**

- \| SNOW-207061 | Fixed a problem with null checking when converting to timestamp values.

**JDBC Driver 3.12.14**

- \| SNOW-150601 | Suppressed a warning about Illegal reflective access.
- \| SNOW-163265 | Fixed an issue when the getDate method passed in a Calendar object.
- \| SNOW-201788 | Prevented attempts to drop a column that is part of clustering key when generating a temp table.
- \| SNOW-204185 | Fixed an edge case in which ResultSet.next() can hang when the ResultSet data downloader threads hit unexpected errors.
- \| SNOW-208695 | Updated the junit version from 4.12 to 4.13.1.

**JDBC Driver 3.12.13**

- \| SNOW-194437 | INSERT of TIME type bind values via JDBC does not trigger bulk array load via stage.
- \| SNOW-161733 | JDBC setString function performance is not satisfactory.
- \| SNOW-195989 | Snowflake JDBC driver is unable to connect via Key-Pair authentication if they private key is encrypted and if the key is generated using OpenSSL 1.1.1g on Windows or Ubuntu.
- \| SNOW-199080 | Yearly GPG key rotation.

**JDBC Driver 3.12.12**

- \| SNOW-180303 | Removed unnecessary log lines from the JDBC Driver log.

**JDBC Driver 3.12.11**

- \| SNOW-182450 | Added in-band telemetry metrics for the DatabaseMetaData get methods (e.g. getTables).
- \| SNOW-176125 | When the log level is set to DEBUG, log the OOB telemetry entries that are sent to Snowflake.

**JDBC Driver 3.12.10**
- \| SNOW-136193 | JDBC is adjusting DLS for some values that are towards the DLS switch, resulting in a wrong result
- \| SNOW-164505 | Implement telemetry events in JDBC driver where needed
- \| SNOW-170758 | SAML/SSO works in Snowflake UI but does not work with SnowSQL or JDBC when using ExternalBrowser

**JDBC Driver 3.12.9**

- \| SNOW-170944 | Enhance the way how JDBC masks sensitive data.
- \| SNOW-171834 | Cut down on redundant parameters in ClientEnv field

**JDBC Driver 3.12.8**

- \| SNOW-164084 | Reverted a fix that broke OOB telemetry for the driver.
- \| SNOW-163938 | Resolve telemetry response failures in JDBC driver
- \| SNOW-163587 | DatabaseMetaData getFunctionColumns(null, "%", "%", "%") throws SQL compilation error
- \| SNOW-165718 | Add log of client parameter settings to be accessed in Snowhouse
- \| SNOW-169174 | CVE-2020-14061, CVE-2020-14062, CVE-2020-14060, CVE-2020-14195 com.fasterxml.jackson.core:jackson-databind to version 2.9.10.5 or later

**JDBC Driver 3.12.7**

- \| N/A         | Version is not available for download; all fixes are available in 3.12.8 (and higher).

**JDBC Driver 3.12.6**

- \| SNOW-146005 | Fixed issue where using the Spring Data JDBC ``SimpleJdbcInsert`` class to execute a SQL statement threw an exception.
- \| SNOW-150921 | Fixed issue where the connection was retrying the OCSP endpoint while using ``insecureMode``; updated the driver to use OCSP mode specified in the session config for OOB telemetry.
- \| SNOW-152748 | Added new connection parameter, ``stringsQuotedForColumnDef``, to support changes to how the ``DatabaseMetaData.getColumns()`` and ``DatabaseMetaData.getProcedureColumns()`` methods return COLUMN_DEF string values with or without single quotes.
- \| SNOW-157758 | Internal fix for pending feature.
- \| SNOW-163080 | Performance improvements for calling ``ResultSet.next()`` after queries have completed.

**JDBC Driver 3.12.5**

- \| SNOW-115446 | JDBC: Wrong File Name When Using compressAndUploadStream Method on GCP Deployment
- \| SNOW-152637 | Investigate/add metrics to see when asynchronous querying is used
- \| SNOW-150593 | Snowflake throws ConcurrentModificationException when attempting to close resultSets #212
- \| SNOW-153278 | Confirm IDToken presence in JDBC Debug Logs
- \| SNOW-154633 | Create javadoc of JDBC and add all JDBC licenses in it
- \| SNOW-154927 | CVE-2020-10969 CVE-2020-9546 CVE-2020-11620 CVE-2020-10672: Vulnerable versions: >= 2.9.0, <= 2.9.10.3 Patched version: 2.9.10.4
- \| SNOW-156092 | Fix Spark/JDBC nullpointer error in getObject()
- \| SNOW-158363 | Snowflake Authentication Token sneaking through secret detector in logs
- \| SNOW-155630 | Fix some async query PrPr issues and add metadata retrieval

**JDBC Driver 3.12.4**

- \| SNOW-146005 | Spring JDBC bug for Snowflake
- \| SNOW-136193 | JDBC is adjusting DLS for some values that are towards the DLS switch, resulting in a wrong result
- \| SNOW-153256 | Add more verbose error handling to telemetry services
- \| SNOW-86734 | Add client information to USER-AGENT HTTP header
- \| SNOW-153485 | Implemented asynchronous querying in JDBC driver
- \| SNOW-143877 | Support for a custom type name in ResultSetMetadata.getColumnTypeName(); to be used for new types.      

**JDBC Driver 3.12.3**

- \| SNOW-75286 | Hide Sensitive data from logs and exceptions for JDBC
- \| SNOW-117429 | Remove Result JSON parser v1 from JDBC.
- \| SNOW-144823 | Fix memory Leak with Telemetry Service's shutdown hook
- \| SNOW-147672 | CVE-2020-8840 CVE-2019-20330 Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.10.3 or later.

**JDBC Driver 3.12.2**

- \| SNOW-121867 | SnowflakeConnectionV1.uploadStream() - automatically appends '@' even to correct stage names - unable to upload using escaped internal table stage #199
- \| SNOW-142833 | CVE-2019-20330 CVE-2020-8840 Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.10.3 or later.

**JDBC Driver 3.12.1**

- \| SNOW-29974  | Add binding support for TIMESTAMP_TZ including Timezone
- \| SNOW-128360  | Fix NoSuchMethodError: org.slf4j.helpers.MessageFormatter.arrayFormat for Matlab
- \| SNOW-134689 | Increase multi part upload threshold to 64MB for PUT command

**JDBC Driver 3.12.0**

- \| SNOW-68471  | Introduce CLIENT_METADATA_USE_SESSION_DATABASE to scope the database for metadata access. false by default.
- \| SNOW-125221 | Fix getStringFunctions() that does not return all support string functions
- \| SNOW-122286 | AWS: When OVERWRITE is false, which is set by default, the file is uploaded if no same file name exists in the stage. This used to check the content signature but it will no longer check. Azure and GCP already work this way.
- \| SNOW-124868 | Add new statement types for LIST, GET, PUT, RM
- \| SNOW-103629 | Use the FIPS S3 endpoints for regions in FIPS mode
- \| SNOW-128360 | Fix slf4j compatibility issue with Matlab

**JDBC Driver 3.11.1**

- \| SNOW-126957 | Add CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS for logging statements and binding data in INO log level.
- \| SNOW-122023  | Fix the order of escapeChars for getTables and getColumns.
- \| SNOW-123702 | Update BouncyCastle to 1.60 to fix two high severity issues
- \| SNOW-124928 | Fix precision loss while using getFloat/getDouble for Decimal values having large scale
- \| SNOW-121276 | Add ability to serialize SnowflakeDataSource objects

**JDBC Driver 3.11.0**

- \| SNOW-84438 | GA: ARROW format support, to be enabled in the next few weeks
- \| SNOW-105117 | Fix JDBC Failures retrieving results on GCP
- \| SNOW-119801 | Upgrade JDBC's arrow lib to 0.15.1
- \| SNOW-115434 | Added in writeable check on file cache and change to the home directory if not writable.
- \| SNOW-116121 | Fix JDBC result set produces wrong result for date 0200-02-28
- \| SNOW-98693 | Implement DriverPropertyInfo
- \| SNOW-70240 | Add connection parameter helps to the JDBC command line
- \| SNOW-65944 | Connection.supportsTransactionIsolationLevel() returned not supported
- \| SNOW-115735 | Reduce alter session set autocommit
- \| SNOW-75486 | Add support of keypair parameters in JDBC connection string
- \| SNOW-119059 | Improve error message when required proxy parameter is missing
- \| SNOW-120495 | Add support for OAuth token to SnowflakeBasicDataSource #194
- \| SNOW-70240  | Add connection parameter helps to the JDBC command line

**JDBC Driver 3.10.3**

- \| SNOW-110357 | Fix CVE-2019-16942
- \| SNOW-110744 | Fix array batch is not usable if number of records*fields in a batch is large #186
- \| SNOW-86551 | Fix bugs related to GS generated Arrow results and queries with subqueries
- \| SNOW-97749 | Enable JDBC ResultSet distributed process to support proxy

**JDBC Driver 3.10.2**

- \| SNOW-102750 | Increasing the max limit connection to 300 for JDBC driver.
- \| SNOW-96797 | Support Arrow for select query results generated by GS
- \| SNOW-109827 | Fix bug in JDBC sample code hang
- \| SNOW-104007 | Fix CVE with nimbusds < 7.9

**JDBC Driver 3.10.1**

- \| SNOW-99312 | Implement better retry functionality for chunk downloader
- \| SNOW-98272 | Enable OVERWRITE option for PUT command to overwrite the files
- \| SNOW-23970 | Support wildcards in directory names in PUT commands
- \| SNOW-99497 | Add session id to SnowflakeConnection
- \| SNOW-99630 | Fix CVE-2019-16335, CVE-2019-14540
- \| SNOW-99954 | Associate describe and execute jobs for the server

**JDBC Driver 3.10.0**

- \| SNOW-94386 | Fix getShort, getInt, getLong, getBigDecimal, getFloat, getDouble, getBytes to be consistent between JSON and ARROW result sets
- \| SNOW-97598 | Fix special Characters in Table Name causes getColumns() to not return values
- \| SNOW-97684 | Async submit in-band telemetry data
- \| SNOW-97215 | Change Prepare statement to defer SQL syntax and binding value check to Execute to improve the latency
- \| SNOW-99630 | Fix CVE-2019-16335, CVE-2019-14540: com.fasterxml.jackson.core:jackson-databind

**JDBC Driver 3.9.2**

- \| SNOW-91553 | Refactor for JDBC ResultSet distributed processing
- \| SNOW-88820 | Add cross type tests to JDBC
- \| SNOW-90601 | Add GCS PUT and GET test cases
- \| SNOW-91578 | Fix NullPointerException in TelemetryService.java in SnowflakeFileTransferAgent.java
- \| SNOW-92223 | Merge ArrowLogger and ArrowLogFactory to Arrow source code
- \| SNOW-90927 | Fix AccessControlException in SFResultSet.next()
- \| SNOW-91271 | Fix prepareStatement(String sql, int autoGeneratedKeys) that throws SQLFeatureNotSupportedException
- \| SNOW-90968 | Fix NullPointerException in calling resultSet.getTimestamp() on Time column with null value
- \| SNOW-74252 | Fix calculateUpdateCount(SFBaseResultSet resultSet) that has updateCount as int limited to 4B implying 2.1B records limit
- \| SNOW-94341 | Deprecate Arrow format for JDBC version older than 3.9.1
- \| SNOW-94387 | Fix JDBC Arrow Result: getBoolean, getShort, getInt, getLong, getBigDecimal, getFloat, getDouble, getBytes
- \| SNOW-95458 | Loosen the test interval constraint in SFFormatterTest.java
- \| SNOW-96157 | Add SnowflakeConnection interface

**JDBC Driver 3.9.1**

- \| SNOW-90169 | Fix OCSP fail open
- \| SNOW-84419 | Support proxy for Azure in JDBC (host and port only. No user and password is supported)
- \| SNOW-90230 | Flush revoked OCSPExceptionTelemetryEvent immediately
- \| SNOW-92525 | Make Arrow lib compatible with Java 8

**JDBC Driver 3.9.0**

- \| SNOW-90644 | Add Statement.getLargeCount and executeLargeUpdate
- \| SNOW-86243 | Add Parameter to control Multi-Statement Support with Count
- \| SNOW-75648 | Add validateDefaultParameters to validate the database, schema and warehouse at connection time. false by default.
- \| SNOW-85191 | Fixed DatabaseMetaData.getColumns returns empty string on COLUMN_DEF for columns with no defaults
- \| SNOW-86345 | Add PrivateKey based authentication with datasource
- \| SNOW-88426 | Fix setObject and setNull in the PrepareStatement results into error using latest JDBC driver
- \| SNOW-88467 | Remove javax.activation from jdbc
- \| SNOW-88628 | Fix getTime() method returns NullPointerException error when reading nulls
- \| SNOW-88756 | Fix the return format for VARIANT type with ARROW is some different to that with JSON.
- \| SNOW-89066 | Fix failures to decode the value for datatype time(0) if the result format is ARROW.
- \| SNOW-89110 | Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.9.2 to fix security vulnerability.
- \| SNOW-89737 | Fix ResultSet from Arrow_force format does not match resultSet from JSON format after calling executeQuery()
- \| SNOW-90009 | Upgrade org.apache.tika:tika-core to version 1.22 to fix security vulnerability
- \| SNOW-90431 | Fix OOB throwing NPE or provides wrong context in multithread scenarios

**JDBC Driver 3.8.8**

- \| SNOW-79383 | Implement CallableStatement
- \| SNOW-87251 | Added result_query_format parameter for the private preview of new result set format
- \| SNOW-87589  | Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.9.1 or later to fix security vulnerability.

**JDBC Driver 3.8.7**

- \| SNOW-85251 | Increase the adjust steps in conservative memory usage mode to better utilize result cache
- \| SNOW-83429 | Build JDBC driver with FIPS certified Bouncy Castle libraries
- \| SNOW-83815 | Query id no longer accessible via JDBC as of 3.7.1
- \| SNOW-84396 | Types.SMALLINT not supported in getColumnClassName

**JDBC Driver 3.8.6**

- \| SNOW-84683 | Add oktausername parameter support to OKTA authenticator

**JDBC Driver 3.8.5**

- \| SNOW-82723 | Support proxyHost including dash and dot
- \| SNOW-84129 | JDBC turn on CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE except prod for testing
- \| SNOW-83666 | PUT to S3 endpoint return timeout when using a JDBC connection through proxy
- \| SNOW-84396 | Types.SMALLINT not supported in getColumnClassName

**JDBC Driver 3.8.4**

- \| SNOW-38957 | Connection errors will return multiple error codes instead of 200002
- \| SNOW-70888 | Update Client Driver OCSP Endpoint URL for Private Link Customers
- \| SNOW-19476 | Implement DatabaseMetadata.getTablePrivileges
- \| SNOW-80773 | Connection.setClientInfo refuses any parameter
- \| SNOW-81015 | proxyUser and proxyPassword are optional in the JDBC connect string.
- \| SNOW-81829 | Use Standard Connection Fields for Global URL
- \| SNOW-78996 | Remove https from account name if specified.
- \| SNOW-74255 | Implement java.sql.Statement.executeLargeBatch

**JDBC Driver 3.8.3**

- \| SNOW-70888 | JDBC OCSP URL Update for Privatelink

**JDBC Driver 3.8.2**

- \| SNOW-62766 | Deprecate CLIENT_RESULT_PREFETCH_THREADS and CLIENT_RESULT_PREFETCH_SLOTS
- \| SNOW-77592 | Implemented getProcedures and getProcedureColumns
- \| SNOW-79011 | JDBC don't surface errors when the session is gone
- \| SNOW-79125 | Key comparison should be done by equals method instead of double equal signs
- \| SNOW-79699 | Upgrade com.fasterxml.jackson.core:jackson-databind to version 2.9.9 or later
- \| SNOW-80208 | Fixed a missing data bug on JDBC 3.7.1+’s resultChunkV2: strictly clean isNulls while using from the cache

**JDBC Driver 3.8.1**

- \|SNOW-76035 | DML returns the number of updated rows in getUpdateCount() otherwise -1
- \|SNOW-70751 | Connection.setClientInfo for JDBC to support ApplicationName
- \|SNOW-74086 | Implement DatabaseMetaData.getFunctionColumns
- \|SNOW-76375 | Implement PreparedStatement.getParameterMetaData(), ParameterMetaData.getParameterCount() ParameterMetaData.getParameterType(int)
- \|SNOW-77987 | Revoked OCSP Response persists in in-memory cache
- \|SNOW-67078 | executeBatch supports PUT and GET
- \|SNOW-79011 | Ignore session is missing error when closing connection

**JDBC Driver 3.8.0**

- \|SNOW-75285|Remove sensitive data from URL for JDBC logging
- \|SNOW-75925|Create JDBC interfaces SnowflakeStatement, SnowflakeResultSet, and, SnowflakePreparedStatement to expose Snowflake specific APIs: SnowflakeStatement.getQueryID(), SnowflakeStatement.getBatchQueryID(), SnowflakeResultSet.getQueryID(), SnowflakePreparedStatement.getQueryID()
- \|SNOW-76010|Updated c3p0 version for tests
- \|SNOW-76375|Implements DataBaseMetaData.getParameterMetaData() and ParameterMetaData.getType()
- \|SNOW-75285|Scrub secrets before logging
- \|SNOW-77160|Add OCSP_MODE metric
- \|SNOW-74086|Add getFunctionColumns
- \|SNOW-76150|OCSP SoftFail support for JDBC

**JDBC Driver 3.7.2**

- \|SNOW-67615| Apply CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX behavior to all JDBC get functions with catalog and schema as inputs
- \|SNOW-68058| CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY for JDBC
- \|SNOW-73034| Setting the index 0 for binding is ignored. It should raise an exception.
- \|SNOW-68756| JAVA heap space error when querying data: provide detailed error message and suggestions when hits OOM
- \|SNOW-70356| Ensure all associated objects are freed when closed.
- \|SNOW-70409| Close all associated objects when the parent object is closed.
- \|SNOW-71689| Update Client Driver to use new OCSP Endpoint URL based on Client Failover
- \|SNOW-73555| Fixed Not dropping unselected columns after creating temp table
- \|SNOW-67871| Add getQueryId() method to SnowflakeStatementV1 and SnowflakeResultSetV1
- \|SNOW-74238| JDBC SnowflakeBasicDatasource use a driver does not comes from Snowflake

**JDBC Driver 3.7.1**

- \| SNOW-73421 | Internal change for future improvement
- \|SNOW-70354 | Throw SQLException when calling methods of the closed objects.

**JDBC Driver 3.7.0**

- \|SNOW-65887|Change source and target Java version to 1.8 for JDBC driver

**JDBC Driver 3.6.28**

- \|SNOW-67095|Fix a bug which caused the 3.6.x JDBC Driver hangs when resultSet is not consumed. The JDBC driver now always releases resultSet and its memory usage when a statement is closed.
- \|SNOW-67120|Change getTableTypes() from only returning TABLE and VIEW to including TEMPORARY and TRANSIENT types.
- \|SNOW-66302|Fixed parsing date and time format issue.

**JDBC Driver 3.6.27**

- \|SNOW-42661| Add unknown type  binding variable support in table UDF
- \|SNOW-66840| Align CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX behavior of JDBC with ODBC
- \|SNOW-67327| NPE when timestamp value is null in binding
- \|SNOW-62511| Mask AWS password in a query

**JDBC Driver 3.6.26**

- \|SNOW-66026|Change all INFO and WARNING logging to DEBUG in JDBC
- \|SNOW-66015|Binary column always returns 0 precision
- \|SNOW-65421|Fixed binding null with data type raising error.
- \|SNOW-65154|Fixed CHANGELOG.rst format
- \|SNOW-64977|More granular error message for failed batch loads
- \|SNOW-64063|Update JDBC Loader API to using CREATE TABLE ... LIKE syntax
- \|SNOW-53174|Improve error messages when a driver fails to connect to Snowflake, S3 or OCSP

**JDBC Driver 3.6.25**

- \|SNOW-64564| Lazy init JDBC legacy logger
- \|SNOW-63813| Explicitly allows all proxy data to be fed via the JDBC connection string
- \|SNOW-64570| Failed to serialize ClientAuthnDTO in boomi cloud environment

**JDBC Driver 3.6.24**

- \|SNOW-63844| Security vulnerability: com.fasterxml.jackson.core:jackson-databind >= 2.9.0, < 2.9.8
- \|SNOW-62247| Add enum REJECTED_RECORD in class LoadingError
- \|SNOW-61650| Support Dell boomi cloud

**JDBC Driver 3.6.23**

- \|SNOW-63523| Removed hard-coded ``Level.ALL`` for logger initialization.
- \|SNOW-63481| Security enhancement: Updated ``tika-core`` to 1.20.
- \|SNOW-63341| Driver no longer throws an incident for a ``no row found`` user error.
- \|SNOW-63240| Added additional null checks in statements.
- \|SNOW-63137| Changed default driver log level from ``ALL`` to ``INFO`` in the ``logging.properties`` example (in the Snowflake documentation).
- \|SNOW-63067| Fixed issue with intermittent error in driver even though the **Query Details** page (in the web interface) shows the query was successful.
- \|SNOW-61210| Improved OCSP Cert Auth and Handshake retry.
- \|SNOW-45402| Added support for 256-bit encryption for Azure stages.

**JDBC Driver 3.6.22**

- \|SNOW-63026| Driver now invalidates outdated OCSP responses when checking the cache.
- \|SNOW-62996| Fixed intermittent JDBC connection failure in PrivateLink.
- \|SNOW-62140| The default setting for CLIENT_MEMORY_LIMIT parameter is now dynamic, based on the amount of system memory available.
- \|SNOW-61424| Removed unnecessary/redundant version logs.
- \|SNOW-54606| Fixed issue that caused the following exception when using the driver with Java Spring Boot: ``Caused by: java.lang.IllegalArgumentException: URL must start with 'jdbc'``.
- \|SNOW-63163| Fixed NPE when fetching data.

**JDBC Driver 3.6.21**

- \|SNOW-61862| Driver now uses ``YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM`` for timestamp array binding.

**JDBC Driver 3.6.20**

- \|SNOW-61209| Fixed performance issue with result set download.

**JDBC Driver 3.6.19**

- \|SNOW-44393| Driver now catches exceptions for ``prepareStatement`` so that execution can continue.

**JDBC Driver 3.6.17**

- \|SNOW-59862| Added JVM properties for CLIENT_PREFETCH_THREADS, CLIENT_MEMORY_LIMIT, and CLIENT_RESULT_CHUNK_SIZE.
- \|SNOW-58812| Fixed issue with PARALLEL parameter for PUT and GET commands.
- \|SNOW-59802| Fixed issue with wrong TIME format for the Loader API.
- \|SNOW-56081| CLOB data type now mapped to STRING data type in Snowflake.

**JDBC Driver 3.6.16**

- \|SNOW-57107| Driver now checks to ensure all dependencies are correctly shaded.
- \|SNOW-56603| As announced previously, the CLIENT_MEMORY_LIMIT parameter now governs the total memory used per process instead of per query.

**JDBC Driver 3.6.15**

- \|SNOW-56771| Implemented shading of additional dependencies to a new location to mitigate package conflicts.
- \|SNOW-57051| Fixed CVE-2018-11761.
- \|SNOW-56836| Added support for nanoseconds precision for TIMESTAMP data in Loader API.
- \|SNOW-56604| Added ``skipParsing`` option to ``prepareStatement`` method to skip fetching metadata.

**JDBC Driver 3.6.14**

- \|SNOW-55075| Introduced the CLIENT_RESULT_COLUMN_CASE_INSENSITIVE session parameter to enable matching case-sensitivity for column names in ``ResultSet``.

**JDBC Driver 3.6.13**

- \|SNOW-55868| Added service name support for multi-GS clustering (internal feature).
- \|SNOW-55138| Upgraded dependency to fix known vulnerabilities.
- \|SNOW-55095| Internal change for pending feature.
- \|SNOW-54926| Added ``compressFileByPut``, ``compressDataBeforePut``, ``compressLevel`` to Loader API.
- \|SNOW-55035| Added ``request_guid`` for HTTP request tracing.

**JDBC Driver 3.6.12**

- \|SNOW-26324| Added ``--version`` option to return the JDBC driver version and additional information.

**JDBC Driver 3.6.11**

- \|SNOW-53650| Internal change for pending feature.
- \|SNOW-53452| Internal change for pending feature.

**JDBC Driver 3.6.10**

- \|SNOW-52486| Fixed ``clientStartTime`` and ``retryCount`` metrics in ``query-request``.
- \|SNOW-50766| Updated driver to enforce virtual host style for S3 URLs.
- \|SNOW-50717| Fixed ``SQLException`` in ``getFunctionColumns`` API call.
- \|SNOW-45419| Changed the heartbeat frequency to hourly to mitigate issue with token expiration.
- \|SNOW-39748| Disabled cookie management.

**JDBC Driver 3.6.9**

- \|SNOW-51691| Added support for specifying file cache directories as environment variables or JVM system properties.
- \|SNOW-49850| Added support for disabling SOCKS proxy for JDBC traffic using a new connection parameter (``disableSocksProxy``).
- \|SNOW-41673| Added ``retryCount`` and ``clientStartTime`` parameters to ``query-request`` requests for JDBC.

**JDBC Driver 3.6.8**

- \|SNOW-49653| Internal change for pending feature.

**JDBC Driver 3.6.7**

- \|SNOW-50141| Fixed issue with ``setObject`` not handling BOOLEAN data type.
- \|SNOW-49982| Added ``onError`` parameter in the Loader API; corresponds to the ``ON_ERROR`` option in the COPY INTO *<table>* command.
- \|SNOW-49850| Upgraded AWS SDK to enable support for disabling socket proxy.
- \|SNOW-49653| Internal change for pending feature.

**JDBC Driver 3.6.6**

- \|SNOW-50032| Fixed issue with the OCSP retry check, which wasn't performed if the validity check failed. 

**JDBC Driver 3.6.5**

- \|SNOW-45631| Improved array binding when routing bind values through stage.
- \|SNOW-45545| Fixed issue with the data-to-CSV converter for the Loader API. ``NULL`` and empty values are now correctly converted to ``NULL`` and empty, respectively.
- \|SNOW-45021| Removed login name requirement when authenticating with an OAuth access token.

**JDBC Driver 3.6.4**

- \|SNOW-45612| Added ``authenticator`` setting to ``SnowflakeBasicDatasource``.
- \|SNOW-45600| Driver now closes the HTTP response stream to allow HTTP client to reuse socket.
- \|SNOW-45484| Fixed calculation for 50MB file size.
- \|SNOW-41096| Added a parameter to control Telemetry service (for pending feature in the Spark Connector).

**JDBC Driver 3.6.3**

- \|SNOW-43251| Fixed erroneous exception raised when COPY statement loads 0 files.

**JDBC Driver 3.6.2**

- \|SNOW-44536| Fixed the Loader API to support CSV filenames that contain spaces.
- \|SNOW-44497| Fixed the Loader API to suppress race conditions for date formatting.
- \|SNOW-44405| Added ``copyEmptyFieldAsEmpty`` to the Loader API to support ``EMPTY_FIELD_AS_NULL=false`` option for COPY command.

**JDBC Driver 3.6.1**

- \|SNOW-43215| Updated the driver to support OCSP dynamic cache server for PrivateLink.

**JDBC Driver 3.6.0**

- \|SNOW-42908| Enabled the automatic class loader for ``SnowflakeDriver`` class. 
- \|SNOW-39684| Enabled the OCSP Response Cache Server by default.

**JDBC Driver 3.5.5**

- \|SNOW-42722| Added support for SF_OCSP_RESPONSE_CACHE_DIR environment variable to specify the OCSP cache file location.
- \|SNOW-39872| Added APPLICATION connection property to allow setting the name for 3rd-party applications.

**JDBC Driver 3.5.4**

- \|SNOW-41484| Fixed URL mismatch error that occurred when using OKTA authentication and the JDBC connection URL contains a port number.

**JDBC Driver 3.5.3**

- \|SNOW-40230| Removed dependency on ``commons-lang3`` package.
- \|SNOW-34464| Added support for key pair authentication.

**JDBC Driver 3.5.2**

- \|SNOW-38455| Upgraded HttpClient to 4.5.5.
- \|SNOW-38454| Upgraded Jackson JSON packages to 2.9.4.

**JDBC Driver 3.5.1**

- \|N\/A| Private release (for internal purposes only; no changes)

**JDBC Driver 3.5.0**

- \|SNOW-38486| Added support for checking for OCSP revocation.
- \|SNOW-37766| Added support for getting ``SecureRandom`` instances without specifying a provider name; this is required because the driver could be running under the IBM JDK.

**JDBC Driver 3.4.3**

- \|SNOW-34464| Internal change for pending feature.

**JDBC Driver 3.4.2**

- \|SNOW-37755| Refactored a server-side fix (SNOW-36580) on the client side.
- \|SNOW-37184| Added support for binding object identifiers.

**JDBC Driver 3.4.1**

- \|SNOW-37400| Added shaded ``amazon.ion`` package.

**JDBC Driver 3.4.0**

- \|SNOW-37276| Fixed an issue where the driver could not use the TLS 1.2 cipher suites in JDK1.7.
- \|SNOW-37242| Allow preparing all types of statements (reverts a change introduced in v3.3.0).
- \|SNOW-37186| Fixed an issue with the NUMBER format in JDBC ``SnowflakeBasicaDataSource.java``.

**JDBC Driver 3.3.3**

- \|SNOW-36917| Fixed an issue where the Loader API incorrectly converted timestamp dates earlier than 1582-Oct-04 due to differences between the Julian and Gregorian calendar.
- \|SNOW-35613| Internal change for pending feature.

**JDBC Driver 3.3.2**

- \|SNOW-32282| Internal change for pending feature.
- \|SNOW-32001| Replaced AWS_ID and AWS_KEY with newer versions.

**JDBC Driver 3.3.1**

- \|SNOW-30511| Fixed issue where Okta returned a 403 error (during federated authentication) due to the driver caching the Okta token in a cookie.

**JDBC Driver 3.3.0**

- \|SNOW-32656| Driver behavior changed to throw an exception if SQL statement cannot be prepared.

**JDBC Driver 3.2.7**

- \|SNOW-32618| Added support for SAML 2.0-compliant services/applications for federated authentication by adding the ``externalbrowser`` option to the ``authenticator`` connection parameter.

**JDBC Driver 3.2.6**

- \|SNOW-31633| Changed ``SFTimestamp`` to accommodate the full range of timestamps supported in Snowflake.

**JDBC Driver 3.2.5**

- \|SNOW-33566| Added support for ``ResultSet.isLast()``, ``isBeforeFirsrt()``, and ``isAfterLast()``.
- \|SNOW-30962| Optimized the driver by combining ``describe`` and ``execute`` methods when there is no bind.

**JDBC Driver 3.2.4**

- \|SNOW-33371| Fixed issue with v3.2.2 of the JDBC driver not working with the internal stage transfer feature for the Spark Connector.
- \|SNOW-33227| Added support for new session parameter, JDBC_TREAT_DECIMAL_AS_INT, which, if set to TRUE (default value) instructs the driver to treat a column whose scale is zero as BIGINT instead of DECIMAL.
- \|SNOW-33042| Added support to driver for PUT/GET over encrypted staged files for MS Azure.

**JDBC Driver 3.2.3**

- \|SNOW-32618| JDBC driver ADFS integration rewritten using socket API.

**JDBC Driver 3.2.2**

- \|SNOW-32618| Added support for SAML 2.0-compliant applications.
- \|SNOW-31703| Added support for MS Azure.

**JDBC Driver 3.2.1**

- \|SNOW-32060| Added support in the Loader API for binding ``java.sql.Time`` with the TIME data type and dropped support for binding ``java.sql.Time`` with TIMESTAMP.

**JDBC Driver 3.2.0**

- \|SNOW-31749| Updated the driver to use AWS SDK 1.11.165.
- \|SNOW-31647| Fixed issue with NUMBER columns that have a scale of 0; they now return BIGINT instead of DECIMAL in the column metadata.
- \|SNOW-30967| Updated the driver to use the latest S3 SDK to provide support for ``proxy`` and ``nonProxy`` JVM options.

**JDBC Driver 3.1.1**

- \|SNOW-31425| Fixed an issue with a missing statement type for ``executeUpdate()``, which caused the statement to fail in USE commands.

**JDBC Driver 3.1.0**

- \|SNOW-31069| Added support for enforcing JDBC driver to use TLS v1.2.
- \|SNOW-30962| Added support for ``executeBatch()`` on prepared DML statements.

**JDBC Driver 3.0.21**

- \|SNOW-15992| Support added for bulk updates using the APIs ``Statement.addBatch()``, ``executeBatch()``, and ``clearBatch()``.

**JDBC Driver 3.0.20**

- \|SNOW-30700| Driver now always uses Gregorian Calendar for DATE, TIME, and TIMESTAMP values in Loader API.
- \|SNOW-18939| Added support for ORC file format in PUT command.

**JDBC Driver 3.0.19**

- \|SNOW-29998| Implemented the basic ``DataSource`` API, which produces a standard ``Connection`` object.
- \|SNOW-21314| Fixed Date value and validity for '0001-01-01'. Previously, it displayed incorrect date output.

**JDBC Driver 3.0.18**

- \|SNOW-30146| Shortened the heartbeat interval to resolve some token expiration issues.

**JDBC Driver 3.0.17**

- \|SNOW-28390| Fixed an issue where JDBC fails to parse an infinite number.
- \|SNOW-26354| Driver returns a ``SQLWarning`` if a non-existent database or schema is specified in the connection properties.

**JDBC Driver 3.0.16**

- \|SNOW-29262| Fixed an issue when calculating time spent on retry.

**JDBC Driver 3.0.15**

- \|SNOW-29141| Fixed a null pointer exception when binding a null value in JDBC.

**JDBC Driver 3.0.14**

- \|SNOW-28882| Fixed issue where null values were returned for 0 values cast to DOUBLE due to the ``wasNull`` flag not being set correctly. 
- \|SNOW-28879| Fixed issue where the result chunk downloader thread prevented the JVM from exiting.

**JDBC Driver 3.0.13**

- \|SNOW-24601| Implemented security patch for federated authentication in JDBC.
- \|SNOW-24184| Open-sourced JDBC Driver on Github.

**JDBC Driver 3.0.12**

- \|SNOW-25540| Added support for binding timestamp variables as timestamp_ntz for applications that use the bind API to load data into datetime columns (which are equivalent to the timestamp_ntz data type).

**JDBC Driver 3.0.11**

- \|SNOW-27255| Fixed internal issue that occurred intermittently if the EventHandler encountered multiple class loaders.

**JDBC Driver 3.0.10**

- \|SNOW-27320| Reverted internal fix from a previous version that caused an issue in this version of the driver.

**JDBC Driver 3.0.9**

- \|SNOW-27121| Fixed an issue where the driver sometimes would hang if it encountered 403 errors while downloading large results. The driver now times out after 1 hour with no response from the application thread during download of results.

**JDBC Driver 3.0.8**

- \|SNOW-25306| Improved performance by using the connection context when retrieving database metadata requests.

**JDBC Driver 3.0.7**

- \|SNOW-26597| Fixed issue where the driver returns an error if the connecting application uses the ``Statement.executeUpdate(String sql, int autoGeneratedKey)`` API because the driver does not support auto-generated keys. The new version of the driver still does not support auto-generated keys; however, if the value for ``autoGeneratedKey`` is ``Statement.NO_KEYS_RETURNED``, the driver now executes the statement successfully.

**JDBC Driver 3.0.6**

- \|SNOW-26298| Fixed issue with invalid UTF-8 returned by driver when extracting data from a table into a file.
- \|SNOW-18758| Forward-slash after the port number is now optional in the URL for the JDBC connect string.

**JDBC Driver 3.0.5**

- \|SNOW-26032| Fixed issue with SNOWFLAKE_SAMPLE_DATABASE not being returned by ``DatabaseMetadata.getCatalogs()`` method.
- \|SNOW-25974| Fixed issue in Windows where PUT command failed if the filename was in quotes and contained backslashes.

**JDBC Driver 3.0.4**

- \|SNOW-14445| Added support for pointing JDBC logger path to a directory other than ``tmp`` to prevent file permission issues.

**JDBC Driver 3.0.3**

- \|SNOW-18243| Added support for case-insensitive searches on column names in result sets. By default, searches are case-sensitive. To request enabling case-insensitive search for your account, please contact `Snowflake Support <https://support.snowflake.net/s/snowflake-support>`_.

**JDBC Driver 3.0.2**

- \|SNOW-25029| Fixed binding support for the TIME data type in the ``PreparedStatement`` API implementation.
- \|SNOW-25024, SNOW-24868| Implemented a fix to generate a user error when the client calls the ``getData``, ``getTimestamp``, or ``getTime`` methods on columns with invalid data types.
- \|SNOW-24947| Fixed issue with GET command when it ends with a semicolon.
- \|SNOW-24610| Updated javadoc related to an issue that caused the Informatica Cloud Snowflake Connector (v1) to fail with the following error: ``invalid data encountered during decompression for file...``.
- \|SNOW-24884| Updated javadoc related to an issue where the Informatica Cloud Snowflake Connector (v1) treated all timestamps as UTC.

**JDBC Driver 3.0.1**

- \|SNOW-24581, SNOW-24569| Fixed issue where an internal error was generated rather than a user error when attempting to convert a data type to an invalid data type.

**JDBC Driver 3.0.0**

- \|SNOW-24544| Added support for AWS Signature JDBC Driver v4.
- \|SNOW-23803| Migrated the classpath from ``com.snowflake ...`` to ``net.snowflake ...``.
- \|SNOW-22351| Improved memory management for downloading large result sets.

**JDBC Driver 2.8.2**

- \|SNOW-24335| Fixed issue where a file upload (PUT command) might not correctly close a file handle that was opened during this operation.
- \|SNOW-21736| Driver now throws a user error instead of generating an incident if a closed ``resultset`` is fetched.

**JDBC Driver 2.8.1**

- \|SNOW-23919| Fixed issue with timezone not being set correctly for the DATE data type, which resulted in date values not being returned correctly.
- \|SNOW-23809| Improved the performance of the ``Connection.getAutoCommit`` API.
- \|SNOW-20904| Driver now available on central ``mvn`` nexus repository.
