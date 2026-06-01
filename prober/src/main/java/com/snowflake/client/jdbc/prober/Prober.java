package com.snowflake.client.jdbc.prober;

import net.snowflake.client.jdbc.SnowflakeConnection;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

public class Prober {
  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz";
  private static final Random random = new Random();
  // Snowflake resource names. Assigned in main() once we know the scope and
  // versions, so each (java_version, driver_version, probe variant) combo
  // owns a stable, deterministic pool of objects -- CREATE OR REPLACE then
  // makes every run idempotent and leak-free.
  private static String stageName;
  private static String stageFilePath;
  private static String tableName;
  private static String javaVersion;
  private static String driverVersion;

  // Legacy random-suffix naming used by the previous version of this prober.
  // generateRandomString(10) emits 10 characters: first uppercased, the rest
  // lowercase from [a-z]. Snowflake stores unquoted identifiers uppercase, so
  // legacy names look like:
  //   TEST_STAGE_[A-Z]{10}       (e.g. TEST_STAGE_ABCDEFGHIJ)
  //   TEST_TABLE[A-Z]{10}        (NOTE: missing underscore -- historical typo;
  //                              see git history for prober/src/.../Prober.java
  //                              between 2025-06-17 and now)
  // The very-first version (commits 3f047afd9, 1098005a1 on 2025-06-09) used
  // unsuffixed literals TEST_STAGE and TEST_TABLE, also covered by the regexes.
  // The new deterministic suffix always contains version separators (e.g.
  // _JAVA_17_0_10_TEM_JDBC_3_24_2), so it never matches these regexes.
  private static final int LEGACY_CLEANUP_MAX_ITERATIONS = 10;
  private static final int LEGACY_CLEANUP_BATCH_SIZE = 1000;
  private static final String LEGACY_STAGE_PREFIX = "TEST_STAGE";
  private static final String LEGACY_TABLE_PREFIX = "TEST_TABLE";
  private static final String LEGACY_STAGE_REGEX = "^TEST_STAGE(_[A-Z]{10})?$";
  private static final String LEGACY_TABLE_REGEX = "^TEST_TABLE([A-Z]{10})?$";

  enum Status {
    SUCCESS(0),
    FAILURE(1);

    private final int code;

    Status(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }
  }

  enum Scope {
    LOGIN,
    PUT_FETCH_GET,
    PUT_FETCH_GET_FAIL_CLOSED
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> arguments = parseArguments(args);

    String url = "jdbc:snowflake://" + arguments.get("host");
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
    setPrivateKey(props);
    setupLogging(props);

    javaVersion = props.getProperty("java_version");
    driverVersion = props.getProperty("driver_version");

    String scope = props.getProperty("scope", "");
    // Deterministic resource names per (java_version, driver_version, variant)
    // -- bounded pool, no leak per run. The PUT_FETCH_GET_FAIL_CLOSED variant
    // gets a "_fail_closed" suffix so it never collides with the regular
    // variant when both run against the same schema.
    String variant = Scope.PUT_FETCH_GET_FAIL_CLOSED.name().toLowerCase().equals(scope)
        ? "fail_closed"
        : "";
    initResourceNames(variant);

    if (Scope.LOGIN.name().toLowerCase().equals(scope)) {
      testLogin(url, props);
    }
    if (Scope.PUT_FETCH_GET.name().toLowerCase().equals(scope)) {
      testPutFetchGet(url, props);
    }
    if (Scope.PUT_FETCH_GET_FAIL_CLOSED.name().toLowerCase().equals(scope)) {
      testPutFetchGetFailClosed(url, props);
    }
  }

  private static void initResourceNames(String variant) {
    String suffix = getResourceSuffix(variant);
    stageName = "test_stage_" + suffix;
    tableName = "test_table_" + suffix;
    stageFilePath = "test_file_" + suffix + ".txt";
  }

  private static String getResourceSuffix(String variant) {
    StringBuilder sb = new StringBuilder();
    sb.append("java_").append(sanitizeForIdentifier(javaVersion));
    sb.append("_jdbc_").append(sanitizeForIdentifier(driverVersion));
    if (variant != null && !variant.isEmpty()) {
      sb.append('_').append(sanitizeForIdentifier(variant));
    }
    return sb.toString();
  }

  private static String sanitizeForIdentifier(String value) {
    if (value == null) {
      return "unknown";
    }
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      sb.append(Character.isLetterOrDigit(c) ? Character.toLowerCase(c) : '_');
    }
    return sb.toString();
  }

  private static void testLogin(String url, Properties properties) {
    boolean success;
    try (Connection connection = DriverManager.getConnection(url, properties);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      int result = resultSet.getInt(1);
      success = result == 1;
    } catch (SQLException e) {
      success = false;
      System.err.println(e.getMessage());
      logMetric("cloudprober_driver_java_perform_login", Status.FAILURE);
      System.exit(1);
    }
    logMetric("cloudprober_driver_java_perform_login", success ? Status.SUCCESS : Status.FAILURE);
  }

  private static void testPutFetchGet(String url, Properties properties) {
    try (Connection connection = DriverManager.getConnection(url, properties);
         Statement statement = connection.createStatement()) {
      SnowflakeConnection sfConnection = connection.unwrap(SnowflakeConnection.class);
      List<String> csv = generateCsv(100);
      String csvFile = csv.stream().collect(Collectors.joining(System.lineSeparator()));
      createWarehouse(statement, properties, "cloudprober_driver_java_create_warehouse");
      createDatabase(statement, properties, "cloudprober_driver_java_create_database");
      createSchema(statement, properties, "cloudprober_driver_java_create_schema");
      createDataTable(statement, "cloudprober_driver_java_create_table");
      createDataStage(statement, "cloudprober_driver_java_create_stage");

      uploadFile(sfConnection, csvFile, "cloudprober_driver_java_perform_put");
      loadFileIntoTable(statement, "cloudprober_driver_java_copy_data_from_stage_into_table");
      fetchAndVerifyRows(statement, "cloudprober_driver_java_data_transferred_completely");
      downloadFile(sfConnection, "cloudprober_driver_java_perform_get");
      compareFetchedDataAndFile(statement, csv, "cloudprober_driver_java_data_integrity");

      cleanupResources(statement, "cloudprober_driver_java_cleanup_resources");
      
      csv.clear();
      csvFile = null;
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    } finally {
      System.gc();
    }
  }

  private static void testPutFetchGetFailClosed(String url, Properties properties) {
    Properties failClosedProperties = new Properties();
    failClosedProperties.putAll(properties);
    failClosedProperties.put("ocspFailOpen", "false");
    try (Connection connection = DriverManager.getConnection(url, failClosedProperties);
         Statement statement = connection.createStatement()) {
      SnowflakeConnection sfConnection = connection.unwrap(SnowflakeConnection.class);
      List<String> csv = generateCsv(100);
      String csvFile = csv.stream().collect(Collectors.joining(System.lineSeparator()));
      createWarehouse(statement, failClosedProperties, "cloudprober_driver_java_create_warehouse_fail_closed");
      createDatabase(statement, failClosedProperties, "cloudprober_driver_java_create_database_fail_closed");
      createSchema(statement, failClosedProperties, "cloudprober_driver_java_create_schema_fail_closed");
      createDataTable(statement, "cloudprober_driver_java_create_table_fail_closed");
      createDataStage(statement, "cloudprober_driver_java_create_stage_fail_closed");

      uploadFile(sfConnection, csvFile, "cloudprober_driver_java_perform_put_fail_closed");
      loadFileIntoTable(statement, "cloudprober_driver_java_copy_data_from_stage_into_table_fail_closed");
      fetchAndVerifyRows(statement, "cloudprober_driver_java_data_transferred_completely_fail_closed");
      downloadFile(sfConnection, "cloudprober_driver_java_perform_get_fail_closed");
      compareFetchedDataAndFile(statement, csv, "cloudprober_driver_java_data_integrity_fail_closed");

      cleanupResources(statement, "cloudprober_driver_java_cleanup_resources_fail_closed");
      
      csv.clear();
      csvFile = null;
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    } finally {
      System.gc();
    }
  }


  private static void createDatabase(Statement statement, Properties properties, String metricName) throws SQLException {
    try {
      String databaseName = properties.getProperty("database", "test_db");
      try (ResultSet rs1 = statement.executeQuery("CREATE DATABASE IF NOT EXISTS " + databaseName);
           ResultSet rs2 = statement.executeQuery("USE database " + databaseName)) {
      }
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating database: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createSchema(Statement statement, Properties properties, String metricName) throws SQLException {
    try {
      String schemaName = properties.getProperty("schema", "test_schema");
      try (ResultSet rs1 = statement.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schemaName);
           ResultSet rs2 = statement.executeQuery("USE SCHEMA " + schemaName)) {
      }
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating schema: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createWarehouse(Statement statement, Properties properties, String metricName) throws SQLException {
    try {
      String warehouseName = properties.getProperty("warehouse", "test_wh");
      try (ResultSet rs1 = statement.executeQuery("CREATE WAREHOUSE IF NOT EXISTS " + warehouseName + " WAREHOUSE_SIZE='X-SMALL';");
           ResultSet rs2 = statement.executeQuery("USE WAREHOUSE " + warehouseName)) {
      }
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating warehouse: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void cleanupResources(Statement statement, String metricName) {
    try {
      try (ResultSet rs1 = statement.executeQuery("REMOVE @" + stageName);
           ResultSet rs2 = statement.executeQuery("DROP TABLE IF EXISTS " + tableName);
           ResultSet rs3 = statement.executeQuery("DROP STAGE IF EXISTS " + stageName)) {
      }
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during cleanup: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
    // Best-effort sweep of legacy random-suffix resources left behind by
    // earlier deployments. Never throws -- a sweep failure must not affect
    // the probe outcome. Emits its own cloudprober metric (status + counts)
    // so the sweep is observable in dashboards rather than just stderr.
    cleanupLegacyRandomResources(statement, metricName + "_legacy");
  }

  /**
   * Drops a bounded number of stages and tables left behind by previous
   * prober deployments that used a random suffix.
   *
   * Mirrors the design used in the Python and ODBC probers:
   *   - One Snowflake Scripting block per (kind, prefix) target -- SHOW +
   *     RESULT_SCAN + REGEXP_LIKE + EXECUTE IMMEDIATE DROP loop, all running
   *     server-side so a single JDBC round-trip drops up to BATCH_SIZE
   *     objects per kind regardless of backlog size.
   *   - SHOW has a predictable 10 000-row cap and never raises the
   *     "INFORMATION_SCHEMA returned too much data" error.
   *   - Outer loop runs up to MAX_ITERATIONS sweeps per probe execution to
   *     accelerate draining; breaks early when a sweep drops 0 objects.
   *   - The REGEXP_LIKE tail precisely matches the historical random suffix
   *     ([A-Z]{10}, optionally absent for the very-first 2025-06 literals)
   *     so the new deterministic names -- which always contain version
   *     separator underscores -- are never matched.
   *
   * Emits three cloudprober metrics under `metricPrefix` so the sweep is
   * observable in dashboards (the previous version only wrote to stderr,
   * which is why the activity looked invisible despite running):
   *   - <metricPrefix>            = 0 on success / 1 on uncaught failure
   *   - <metricPrefix>_dropped    = total objects dropped this execution
   *   - <metricPrefix>_iterations = number of sweep iterations actually run
   */
  private static void cleanupLegacyRandomResources(Statement statement, String metricPrefix) {
    int totalDropped = 0;
    int iterationsRun = 0;
    Status status = Status.SUCCESS;
    try {
      for (int iter = 0; iter < LEGACY_CLEANUP_MAX_ITERATIONS; iter++) {
        int iterDropped = 0;
        iterDropped += dropLegacyBatch(statement, "STAGES", "STAGE",
            LEGACY_STAGE_PREFIX, LEGACY_STAGE_REGEX, LEGACY_CLEANUP_BATCH_SIZE);
        iterDropped += dropLegacyBatch(statement, "TABLES", "TABLE",
            LEGACY_TABLE_PREFIX, LEGACY_TABLE_REGEX, LEGACY_CLEANUP_BATCH_SIZE);
        totalDropped += iterDropped;
        iterationsRun++;
        if (iterDropped == 0) {
          break;
        }
      }
    } catch (Exception e) {
      System.err.println("Error during legacy resource cleanup: " + e.getMessage());
      status = Status.FAILURE;
    }
    System.err.println("Legacy cleanup: dropped " + totalDropped
        + " objects across " + iterationsRun + " iteration(s).");
    logMetric(metricPrefix, status);
    logMetricValue(metricPrefix + "_dropped", totalDropped);
    logMetricValue(metricPrefix + "_iterations", iterationsRun);
  }

  private static int dropLegacyBatch(Statement statement, String showKind, String dropKind,
                                     String prefix, String regex, int batchSize) {
    // Notes on the SQL:
    //   - SHOW <kind> returns names ordered by created_on DESC by default.
    //     With CREATE OR REPLACE on every probe run, the active deterministic
    //     stages occupy the top of that list and crowd legacy names off if
    //     we only LIMIT in SHOW. To make sure we actually find legacy names,
    //     we pull up to 10 000 rows (the SHOW maximum), filter to legacy
    //     ones via REGEXP_LIKE, ORDER BY name ASC for deterministic batch
    //     selection across runs, and only then take BATCH_SIZE for dropping.
    //   - LAST_QUERY_ID() is captured into a local variable immediately
    //     after the SHOW so it can't be confused with any other query that
    //     runs later in the block.
    int showLimit = 10000;
    String script =
        "EXECUTE IMMEDIATE $$\n"
        + "DECLARE\n"
        + "    dropped INT DEFAULT 0;\n"
        + "    show_qid VARCHAR;\n"
        + "BEGIN\n"
        + "    SHOW " + showKind + " STARTS WITH '" + prefix + "' LIMIT " + showLimit + ";\n"
        + "    show_qid := LAST_QUERY_ID();\n"
        + "    LET rs RESULTSET := (\n"
        + "        SELECT \"name\" AS object_name\n"
        + "        FROM TABLE(RESULT_SCAN(:show_qid))\n"
        + "        WHERE REGEXP_LIKE(\"name\", '" + regex + "', 'i')\n"
        + "        ORDER BY \"name\"\n"
        + "        LIMIT " + batchSize + "\n"
        + "    );\n"
        + "    LET c1 CURSOR FOR rs;\n"
        + "    FOR row_var IN c1 DO\n"
        + "        EXECUTE IMMEDIATE 'DROP " + dropKind + " IF EXISTS \"' || row_var.object_name || '\"';\n"
        + "        dropped := dropped + 1;\n"
        + "    END FOR;\n"
        + "    RETURN dropped;\n"
        + "END;\n"
        + "$$";
    try (ResultSet rs = statement.executeQuery(script)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
    } catch (SQLException e) {
      System.err.println("Legacy " + dropKind + " cleanup batch failed (prefix="
          + prefix + "): " + e.getMessage());
    }
    return 0;
  }

  private static void compareFetchedDataAndFile(Statement statement, List<String> csv, String metricName) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("select id,name,email from " + tableName + " order by id")) {
      for (int i = 1; i < csv.size(); i++) {
        String csvRow = csv.get(i);
        String[] csvValues = csvRow.split(",", 3);
        int listId = Integer.parseInt(csvValues[0]);
        String listName = csvValues[1];
        String listEmail = csvValues[2];

        if (!resultSet.next()) {
          logMetric(metricName, Status.FAILURE);
          return;
        }
        int dbId = resultSet.getInt(1);
        String dbName = resultSet.getString(2);
        String dbEmail = resultSet.getString(3);

        boolean idMatch = (dbId == listId);
        boolean nameMatch = dbName.equals(listName);
        boolean emailMatch = dbEmail.equals(listEmail);
        if (!(idMatch && nameMatch && emailMatch)) {
          logMetric(metricName, Status.FAILURE);
          return;
        }
      }
      logMetric(metricName, Status.SUCCESS);
    }
  }

  private static String downloadFile(SnowflakeConnection sfConnection, String metricName) throws SQLException {
    try (InputStream downloadStream = sfConnection.downloadStream("@" + stageName, stageFilePath, false);
         BufferedReader reader = new BufferedReader(new InputStreamReader(downloadStream, StandardCharsets.UTF_8))) {
      List<String> lines = reader.lines().collect(Collectors.toList());
      if (lines.size() == 101) {
        logMetric(metricName, Status.SUCCESS);
      } else {
        logMetric(metricName, Status.FAILURE);
      }
      return lines.stream().collect(Collectors.joining(System.lineSeparator()));
    } catch (IOException e) {
      logMetric(metricName, Status.FAILURE);
      throw new SQLException("Error downloading file", e);
    }
  }

  private static void fetchAndVerifyRows(Statement statement, String metricName) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName)) {
      if (resultSet.next()) {
        int rowCount = resultSet.getInt(1);
        boolean success = rowCount == 100;
        logMetric(metricName, success ? Status.SUCCESS : Status.FAILURE);
      } else {
        logMetric(metricName, Status.FAILURE);
      }
    }
  }

  private static void loadFileIntoTable(Statement statement, String metricName) throws SQLException {
    try {
      try (ResultSet rs = statement.executeQuery("copy into " + tableName + " from @" + stageName + "/" + stageFilePath + " FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);")) {
        // ResultSet automatically closed by try-with-resources
      }
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during copy into table: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void uploadFile(SnowflakeConnection sfConnection, String fileContent, String metricName) throws SQLException {
    try {
      sfConnection.uploadStream("@" + stageName, "", new ByteArrayInputStream(fileContent.getBytes()), stageFilePath, false);
      logMetric(metricName, Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during file upload: " + e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createDataTable(Statement statement, String metricName) throws SQLException {
    try {
      try (ResultSet resultSet = statement.executeQuery("CREATE OR REPLACE TABLE " + tableName + " (id int, name text, email text)")) {
        if (resultSet.next()) {
          boolean result = resultSet.getString(1).equals("Table " + tableName.toUpperCase() + " successfully created.");
          logMetric(metricName, result ? Status.SUCCESS : Status.FAILURE);
        } else {
          logMetric(metricName, Status.FAILURE);
        }
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createDataStage(Statement statement, String metricName) throws SQLException {
    try {
      try (ResultSet createStageResult = statement.executeQuery("CREATE OR REPLACE STAGE " + stageName)) {
        if (createStageResult.next()) {
          boolean result = createStageResult.getString(1).equals("Stage area " + stageName.toUpperCase() + " successfully created.");
          logMetric(metricName, result ? Status.SUCCESS : Status.FAILURE);
        } else {
          logMetric(metricName, Status.FAILURE);
        }
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      logMetric(metricName, Status.FAILURE);
      System.exit(1);
    }
  }

  private static void setupLogging(Properties properties) throws IOException {
    String loggingPropertiesString = "handlers=java.util.logging.ConsoleHandler\n.level=" + properties.getProperty("log_level");
    properties.put("JAVA_LOGGING_CONSOLE_STD_OUT", "false");
    try (InputStream propertiesStream = new ByteArrayInputStream(
        loggingPropertiesString.getBytes(StandardCharsets.UTF_8)
    )) {
      LogManager.getLogManager().readConfiguration(propertiesStream);
    }
  }

  private static void logMetric(String metricName, Status status) {
    logMetricValue(metricName, status.getCode());
  }

  private static void logMetricValue(String metricName, long value) {
    System.out.println(metricName + "{java_version=\"" + javaVersion + "\", driver_version=\"" + driverVersion + "\"} " + value);
  }

  private static Map<String, String> parseArguments(String[] args) {
    Map<String, String> parsedArgs = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String currentArg = args[i];

      if (currentArg.startsWith("--")) {
        String key = currentArg.substring(2); // Remove "--"

        // Check if there is a next argument to be the value
        if (i + 1 < args.length) {
          String nextArg = args[i + 1];
          // Ensure the next argument is not another key
          if (!nextArg.startsWith("--")) {
            parsedArgs.put(key, nextArg);
            i++; // Increment i to skip the value argument in the next iteration
          }
        }
      }
    }
    return parsedArgs;
  }

  private static List<String> generateCsv(int numRows) {
    String[] headers = {"ID", "Name", "Email"};
    List<String> csvRows = new ArrayList<>();

    csvRows.add(String.join(",", headers));

    for (int i = 1; i <= numRows; i++) {
      String firstName = generateRandomString(4 + random.nextInt(5));
      String lastName = generateRandomString(5 + random.nextInt(6));

      String fullName = firstName + " " + lastName;
      String email = (firstName + "." + lastName + "@example.com").toLowerCase();

      StringJoiner rowJoiner = new StringJoiner(",");
      rowJoiner.add(String.valueOf(i));
      rowJoiner.add(fullName);
      rowJoiner.add(email);

      csvRows.add(rowJoiner.toString());
    }

    return csvRows;
  }

  private static String generateRandomString(int length) {
    if (length <= 0) {
      return "";
    }
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    builder.setCharAt(0, Character.toUpperCase(builder.charAt(0)));
    return builder.toString();
  }

  private static void setPrivateKey(Properties props) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    String keyStr = new String(Files.readAllBytes(Paths.get(props.getProperty("private_key_file"))), StandardCharsets.UTF_8).trim();
    byte[] keyBytes = Base64.getUrlDecoder().decode(keyStr);

    // Convert the DER bytes to a private key object
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    RSAPrivateCrtKey privateKey = (RSAPrivateCrtKey) keyFactory.generatePrivate(keySpec);
    props.put("privateKey", privateKey);
    // Remove the path from properties so the driver does not try to read it
    props.remove("private_key_file");
  }
}