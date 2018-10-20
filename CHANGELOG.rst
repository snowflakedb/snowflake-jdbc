**JDBC Driver 3.6.14**
|SSNOW-55075| Introduced CLIENT_RESULT_COLUMN_CASE_INSENSITIVE to match column names case-insensitively in ResultSet

**JDBC Driver 3.6.13**
|SNOW-54926| Added ``compressFileByPut``, ``compressDataBeforePut``, ``compressLevel`` to Loader API.
|SNOW-55138| Upgraded dependency to fix known vulnerabilities.
|SNOW-55868| Added service name support for multi GS clustering.
|SNOW-55035| Added ``request_guid`` for HTTP request tracing.
|SNOW-55095| Internal change for pending feature.
**JDBC Driver 3.6.12**
|SNOW-26324| Added ``--version`` option to return the JDBC driver version and additional information.
**JDBC Driver 3.6.11**
|SNOW-53650| Internal change for pending feature.
|SNOW-53452| Internal change for pending feature.
**JDBC Driver 3.6.10**
|SNOW-52486| Fixed ``clientStartTime`` and ``retryCount`` metrics in ``query-request``.
|SNOW-50766| Updated driver to enforce virtual host style for S3 URLs.
|SNOW-50717| Fixed ``SQLException`` in ``getFunctionColumns`` API call.
|SNOW-45419| Changed the heartbeat frequency to hourly to mitigate issue with token expiration.
|SNOW-39748| Disabled cookie management.
**JDBC Driver 3.6.9**
|SNOW-51691| Added support for specifying file cache directories as environment variables or JVM system properties.
|SNOW-49850| Added support for disabling SOCKS proxy for JDBC traffic using a new connection parameter (``disableSocksProxy``).
|SNOW-41673| Added ``retryCount`` and ``clientStartTime`` parameters to ``query-request`` requests for JDBC.
**JDBC Driver 3.6.8**
|SNOW-49653| Internal change for pending feature.
**JDBC Driver 3.6.7**
|SNOW-50141| Fixed issue with ``setObject`` not handling BOOLEAN data type.
|SNOW-49982| Added ``onError`` parameter in the ``Loader`` API; corresponds to the ``ON_ERROR`` option in the COPY INTO *<table>* command.
|SNOW-49850| Upgraded AWS SDK to enable support for disabling socket proxy.
|SNOW-49653| Internal change for pending feature.
**JDBC Driver 3.6.6**
|SNOW-50032| Fixed issue with the OCSP retry check, which wasn't performed if the validity check failed. 
**JDBC Driver 3.6.5**
|SNOW-45631| Improved array binding when routing bind values through stage.
|SNOW-45545| Fixed issue with the data-to-CSV converter for the ``Loader`` API. ``NULL`` and empty values are now correctly converted to ``NULL`` and empty, respectively.
|SNOW-45021| Removed login name requirement when authenticating with an OAuth access token.
**JDBC Driver 3.6.4**
|SNOW-45612| Added ``authenticator`` setting to ``SnowflakeBasicDatasource``.
|SNOW-45600| Driver now closes the HTTP response stream to allow HTTP client to reuse socket.
|SNOW-45484| Fixed calculation for 50MB file size.
|SNOW-41096| Added a parameter to control Telemetry service (for pending feature in the Spark Connector).
**JDBC Driver 3.6.3**
|SNOW-43251| Fixed erroneous exception raised when COPY statement loads 0 files.
**JDBC Driver 3.6.2**
|SNOW-44536| Fixed Loader API to support CSV filenames that contain spaces.
|SNOW-44497| Fixed Loader API to suppress race conditions for date formatting.
|SNOW-44405| Added ``copyEmptyFieldAsEmpty`` to Loader API to support ``EMPTY_FIELD_AS_NULL=false`` option for COPY command.
**JDBC Driver 3.6.1**
|SNOW-43215| Updated the driver to support OCSP dynamic cache server for PrivateLink.
**JDBC Driver 3.6.0**
|SNOW-42908| Enabled the automatic class loader for ``SnowflakeDriver`` class. 
|SNOW-39684| Enabled the OCSP Response Cache Server by default.
**JDBC Driver 3.5.5**
|SNOW-42722| Added support for SF_OCSP_RESPONSE_CACHE_DIR environment variable to specify the OCSP cache file location.
|SNOW-39872| Added APPLICATION connection property to allow setting the name for 3rd-party applications.
**JDBC Driver 3.5.4**
|SNOW-41484| Fixed URL mismatch error that occurred when using OKTA authentication and the JDBC connection URL contains a port number.
**JDBC Driver 3.5.3**
|SNOW-40230| Removed dependency on ``commons-lang3`` package.
|SNOW-34464| Added support for key pair authentication.
**JDBC Driver 3.5.2**
|SNOW-38455| Upgraded HttpClient to 4.5.5.
|SNOW-38454| Upgraded Jackson JSON packages to 2.9.4.
**JDBC Driver 3.5.1**
|N/A| Private release (for internal purposes only; no changes)
**JDBC Driver 3.5.0**
|SNOW-38486| Added support for checking for OCSP revocation.
|SNOW-37766| Added support for getting ``SecureRandom`` instances without specifying a provider name; this is required because the driver could be running under the IBM JDK.
**JDBC Driver 3.4.3**
|SNOW-34464| Internal change for pending feature.
**JDBC Driver 3.4.2**
|SNOW-37755| Refactored a server-side fix (SNOW-36580) on the client side.
|SNOW-37184| Added support for binding object identifiers.
**JDBC Driver 3.4.1**
|SNOW-37400| Added shaded ``amazon.ion`` package.
**JDBC Driver 3.4.0**
|SNOW-37276| Fixed an issue where the driver could not use the TLS 1.2 cipher suites in JDK1.7.
|SNOW-37242| Allow preparing all types of statements (reverts a change introduced in v3.3.0).
|SNOW-37186| Fixed an issue with the NUMBER format in JDBC ``SnowflakeBasicaDataSource.java``.
**JDBC Driver 3.3.3**
|SNOW-36917| Fixed an issue where the loader API incorrectly converted timestamp dates earlier than 1582-Oct-04 due to differences between the Julian and Gregorian calendar.
|SNOW-35613| Internal change for pending feature.
**JDBC Driver 3.3.2**
|SNOW-32282| Internal change for pending feature.
|SNOW-32001| Replaced AWS_ID and AWS_KEY with newer versions.
**JDBC Driver 3.3.1**
|SNOW-30511| Fixed issue where Okta returned a 403 error (during federated authentication) due to the driver caching the Okta token in a cookie.
**JDBC Driver 3.3.0**
|SNOW-32656| Driver behavior changed to throw an exception if SQL statement cannot be prepared.
**JDBC Driver 3.2.7**
|SNOW-32618| Added support for SAML 2.0-compliant services/applications for federated authentication by adding the ``externalbrowser`` option to the ``authenticator`` connection parameter.
**JDBC Driver 3.2.6**
|SNOW-31633| Changed ``SFTimestamp`` to accommodate the full range of timestamps supported in Snowflake.
**JDBC Driver 3.2.5**
|SNOW-33566| Added support for ``ResultSet.isLast()``, ``isBeforeFirsrt()``, and ``isAfterLast()``.
|SNOW-30962| Optimized the driver by combining ``describe`` and ``execute`` methods when there is no bind.
**JDBC Driver 3.2.4**
|SNOW-33371| Fixed issue with v3.2.2 of the JDBC driver not working with the internal stage transfer feature for the Spark Connector.
|SNOW-33227| Added support for new session parameter, JDBC_TREAT_DECIMAL_AS_INT, which, if set to TRUE (default value) instructs the driver to treat a column whose scale is zero as BIGINT instead of DECIMAL.
|SNOW-33042| Added support to driver for PUT/GET over encrypted staged files for MS Azure.
**JDBC Driver 3.2.3**
|SNOW-32618| JDBC driver ADFS integration rewritten using socket API.
**JDBC Driver 3.2.2**
|SNOW-32618| Added support for SAML 2.0-compliant applications.
|SNOW-31703| Added support for MS Azure.
**JDBC Driver 3.2.1**
|SNOW-32060| Added support in the Loader API for binding ``java.sql.Time`` with the TIME data type and dropped support for binding ``java.sql.Time`` with TIMESTAMP.
**JDBC Driver 3.2.0**
|SNOW-31749| Updated the driver to use AWS SDK 1.11.165.
|SNOW-31647| Fixed issue with NUMBER columns that have a scale of 0; they now return BIGINT instead of DECIMAL in the column metadata.
|SNOW-30967| Updated the driver to use the latest S3 SDK to provide support for ``proxy`` and ``nonProxy`` JVM options.
**JDBC Driver 3.1.1**
|SNOW-31425| Fixed an issue with a missing statement type for ``executeUpdate()``, which caused the statement to fail in USE commands.
**JDBC Driver 3.1.0**
|SNOW-31069| Added support for enforcing JDBC driver to use TLS v1.2.
|SNOW-30962| Added support for ``executeBatch()`` on prepared DML statements.
**JDBC Driver 3.0.21**
|SNOW-15992| Support added for bulk updates using the APIs ``Statement.addBatch()``, ``executeBatch()``, and ``clearBatch()``.
**JDBC Driver 3.0.20**
|SNOW-30700| Driver now always uses Gregorian Calendar for DATE, TIME, and TIMESTAMP values in Loader API.
|SNOW-18939| Added support for ORC file format in PUT command.
**JDBC Driver 3.0.19**
|SNOW-29998| Implemented the basic ``DataSource`` API, which produces a standard ``Connection`` object.
|SNOW-21314| Fixed Date value and validity for '0001-01-01'. Previously, it displayed incorrect date output.
**JDBC Driver 3.0.18**
|SNOW-30146| Shortened the heartbeat interval to resolve some token expiration issues.
**JDBC Driver 3.0.17**
|SNOW-28390| Fixed an issue where JDBC fails to parse an infinite number.
|SNOW-26354| Driver returns a ``SQLWarning`` if a non-existent database or schema is specified in the connection properties.
**JDBC Driver 3.0.16**
|SNOW-29262| Fixed an issue when calculating time spent on retry.
**JDBC Driver 3.0.15**
|SNOW-29141| Fixed a null pointer exception when binding a null value in JDBC.
**JDBC Driver 3.0.14**
|SNOW-28882| Fixed issue where null values were returned for 0 values cast to DOUBLE due to the ``wasNull`` flag not being set correctly. 
|SNOW-28879| Fixed issue where the result chunk downloader thread prevented the JVM from exiting.
**JDBC Driver 3.0.13**
|SNOW-24601| Implemented security patch for federated authentication in JDBC.
|SNOW-24184| Open-sourced JDBC Driver on Github.
**JDBC Driver 3.0.12**
|SNOW-25540| Added support for binding timestamp variables as timestamp_ntz for applications that use the bind API to load data into datetime columns (which are equivalent to the timestamp_ntz data type).
**JDBC Driver 3.0.11**
|SNOW-27255| Fixed internal issue that occurred intermittently if the EventHandler encountered multiple class loaders.
**JDBC Driver 3.0.10**
|SNOW-27320| Reverted internal fix from a previous version that caused an issue in this version of the driver.
**JDBC Driver 3.0.9**
|SNOW-27121| Fixed an issue where the driver sometimes would hang if it encountered 403 errors while downloading large results. The driver now times out after 1 hour with no response from the application thread during download of results.
**JDBC Driver 3.0.8**
|SNOW-25306| Improved performance by using the connection context when retrieving database metadata requests.
**JDBC Driver 3.0.7**
|SNOW-26597| Fixed issue where the driver returns an error if the connecting application uses the ``Statement.executeUpdate(String sql, int autoGeneratedKey)`` API because the driver does not support auto-generated keys. The new version of the driver still does not support auto-generated keys; however, if the value for ``autoGeneratedKey`` is ``Statement.NO_KEYS_RETURNED``, the driver now executes the statement successfully.
**JDBC Driver 3.0.6**
|SNOW-26298| Fixed issue with invalid UTF-8 returned by driver when extracting data from a table into a file.
|SNOW-18758| Forward-slash after the port number is now optional in the URL for the JDBC connect string.
**JDBC Driver 3.0.5**
|SNOW-26032| Fixed issue with SNOWFLAKE_SAMPLE_DATABASE not being returned by ``DatabaseMetadata.getCatalogs()`` method.
|SNOW-25974| Fixed issue in Windows where PUT command failed if the filename was in quotes and contained backslashes.
**JDBC Driver 3.0.4**
|SNOW-14445| Added support for pointing JDBC logger path to a directory other than ``tmp`` to prevent file permission issues.
**JDBC Driver 3.0.3**
|SNOW-18243| Added support for case-insensitive searches on column names in result sets. By default, searches are case-sensitive. To request enabling case-insensitive search for your account, please contact `Snowflake Support <https://support.snowflake.net/s/snowflake-support>`_.
**JDBC Driver 3.0.2**
|SNOW-25029| Fixed binding support for the TIME data type in the ``PreparedStatement`` API implementation.
|SNOW-25024, SNOW-24868| Implemented a fix to generate a user error when the client calls the ``getData``, ``getTimestamp``, or ``getTime`` methods on columns with invalid data types.
|SNOW-24947| Fixed issue with GET command when it ends with a semicolon.
|SNOW-24610| Updated javadoc related to an issue that caused the Informatica Cloud Snowflake Connector (v1) to fail with the following error: ``invalid data encountered during decompression for file...``.
|SNOW-24884| Updated javadoc related to an issue where the Informatica Cloud Snowflake Connector (v1) treated all timestamps as UTC.
**JDBC Driver 3.0.1**
|SNOW-24581, SNOW-24569| Fixed issue where an internal error was generated rather than a user error when attempting to convert a data type to an invalid data type.
**JDBC Driver 3.0.0**
|SNOW-24544| Added support for AWS Signature JDBC Driver v4.
|SNOW-23803| Migrated the classpath from ``com.snowflake ...`` to ``net.snowflake ...``.
|SNOW-22351| Improved memory management for downloading large result sets.
**JDBC Driver 2.8.2**
|SNOW-24335| Fixed issue where a file upload (PUT command) might not correctly close a file handle that was opened during this operation.
|SNOW-21736| Driver now throws a user error instead of generating an incident if a closed ``resultset`` is fetched.
**JDBC Driver 2.8.1**
|SNOW-23919| Fixed issue with timezone not being set correctly for the DATE data type, which resulted in date values not being returned correctly.
|SNOW-23809| Improved the performance of the ``Connection.getAutoCommit`` API.
|SNOW-20904| Driver now available on central ``mvn`` nexus repository.
