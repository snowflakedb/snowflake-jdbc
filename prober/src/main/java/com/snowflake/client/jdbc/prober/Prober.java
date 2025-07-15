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
  private static final String stageName = "test_stage_" + generateRandomString(10);
  private static final String stageFilePath = "test_file_" + generateRandomString(10) + ".txt";
  private static final String tableName = "test_table" + generateRandomString(10);
  private static String javaVersion;
  private static String driverVersion;

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
    PUT_FETCH_GET
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

    if (Scope.LOGIN.name().toLowerCase().equals(props.getProperty("scope"))) {
      testLogin(url, props);
    }
    if (Scope.PUT_FETCH_GET.name().toLowerCase().equals(props.getProperty("scope"))) {
      testPutFetchGet(url, props);
    }
  }

  private static void testLogin(String url, Properties properties) {
    boolean success;
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
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
      List<String> csv = generateCsv(1000);
      String csvFile = csv.stream().collect(Collectors.joining(System.lineSeparator()));
      createWarehouse(statement, properties);
      createDatabase(statement, properties);
      createSchema(statement, properties);
      createDataTable(statement);
      createDataStage(statement);

      uploadFile(sfConnection, csvFile);
      loadFileIntoTable(statement);
      fetchAndVerifyRows(statement);
      downloadFile(sfConnection);
      compareFetchedDataAndFile(statement, csv);

      cleanupResources(statement);
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  private static void createDatabase(Statement statement, Properties properties) throws SQLException {
    try {
      String databaseName = properties.getProperty("database", "test_db");
      statement.executeQuery("CREATE DATABASE IF NOT EXISTS " + databaseName);
      statement.executeQuery("USE database " + databaseName);
      logMetric("cloudprober_driver_java_create_database", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating database: " + e.getMessage());
      logMetric("cloudprober_driver_java_create_database", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createSchema(Statement statement, Properties properties) throws SQLException {
    try {
      String schemaName = properties.getProperty("schema", "test_schema");
      statement.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schemaName);
      statement.executeQuery("USE SCHEMA " + schemaName);
      logMetric("cloudprober_driver_java_create_schema", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating schema: " + e.getMessage());
      logMetric("cloudprober_driver_java_create_schema", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createWarehouse(Statement statement, Properties properties) throws SQLException {
    try {
      String warehouseName = properties.getProperty("warehouse", "test_wh");
      statement.executeQuery("CREATE WAREHOUSE IF NOT EXISTS " + warehouseName + " WAREHOUSE_SIZE='X-SMALL';");
      statement.executeQuery("USE WAREHOUSE " + warehouseName);
      logMetric("cloudprober_driver_java_create_warehouse", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error creating warehouse: " + e.getMessage());
      logMetric("cloudprober_driver_java_create_warehouse", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void cleanupResources(Statement statement) {
    try {
      statement.executeQuery("REMOVE @" + stageName);
      statement.executeQuery("DROP TABLE IF EXISTS " + tableName);
      logMetric("cloudprober_driver_java_cleanup_resources", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during cleanup: " + e.getMessage());
      logMetric("cloudprober_driver_java_cleanup_resources", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void compareFetchedDataAndFile(Statement statement, List<String> csv) throws SQLException {
    ResultSet resultSet = statement.executeQuery("select id,name,email from " + tableName + " order by id");
    for (int i = 1; i < csv.size(); i++) {
      String csvRow = csv.get(i);
      String[] csvValues = csvRow.split(",", 3);
      int listId = Integer.parseInt(csvValues[0]);
      String listName = csvValues[1];
      String listEmail = csvValues[2];

      if (!resultSet.next()) {
        logMetric("cloudprober_driver_java_data_integrity", Status.FAILURE);
        return;
      }
      int dbId = resultSet.getInt(1);
      String dbName = resultSet.getString(2);
      String dbEmail = resultSet.getString(3);

      boolean idMatch = (dbId == listId);
      boolean nameMatch = dbName.equals(listName);
      boolean emailMatch = dbEmail.equals(listEmail);
      if (!(idMatch && nameMatch && emailMatch)) {
        logMetric("cloudprober_driver_java_data_integrity", Status.FAILURE);
        return;
      }
    }
    logMetric("cloudprober_driver_java_data_integrity", Status.SUCCESS);
  }

  private static String downloadFile(SnowflakeConnection sfConnection) throws SQLException {
    InputStream downloadStream = sfConnection.downloadStream("@" + stageName, stageFilePath, false);
    BufferedReader reader = new BufferedReader(new InputStreamReader(downloadStream, StandardCharsets.UTF_8));
    List<String> lines = reader.lines().collect(Collectors.toList());
    if (lines.size() == 1001) {
      logMetric("cloudprober_driver_java_perform_get", Status.SUCCESS);
    } else {
      logMetric("cloudprober_driver_java_perform_get", Status.FAILURE);
    }
    return lines.stream().collect(Collectors.joining(System.lineSeparator()));
  }

  private static void fetchAndVerifyRows(Statement statement) throws SQLException {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
    if (resultSet.next()) {
      int rowCount = resultSet.getInt(1);
      boolean success = rowCount == 1000;
      logMetric("cloudprober_driver_java_data_transferred_completely", success ? Status.SUCCESS : Status.FAILURE);
    } else {
      logMetric("cloudprober_driver_java_data_transferred_completely", Status.FAILURE);
    }
  }

  private static void loadFileIntoTable(Statement statement) throws SQLException {
    try {
      statement.executeQuery("copy into " + tableName + " from @" + stageName + "/" + stageFilePath + " FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);");
      logMetric("cloudprober_driver_java_copy_data_from_stage_into_table", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during copy into table: " + e.getMessage());
      logMetric("cloudprober_driver_java_copy_data_from_stage_into_table", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void uploadFile(SnowflakeConnection sfConnection, String fileContent) throws SQLException {
    try {
      sfConnection.uploadStream("@" + stageName, "", new ByteArrayInputStream(fileContent.getBytes()), stageFilePath, false);
      logMetric("cloudprober_driver_java_perform_put", Status.SUCCESS);
    } catch (SQLException e) {
      System.err.println("Error during file upload: " + e.getMessage());
      logMetric("cloudprober_driver_java_perform_put", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createDataTable(Statement statement) throws SQLException {
    try {
      ResultSet resultSet = statement.executeQuery("CREATE OR REPLACE TABLE " + tableName + " (id int, name text, email text)");
      if (resultSet.next()) {
        boolean result = resultSet.getString(1).equals("Table " + tableName.toUpperCase() + " successfully created.");
        logMetric("cloudprober_driver_java_create_table", result ? Status.SUCCESS : Status.FAILURE);
      } else {
        logMetric("cloudprober_driver_java_create_table", Status.FAILURE);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      logMetric("cloudprober_driver_java_create_table", Status.FAILURE);
      System.exit(1);
    }
  }

  private static void createDataStage(Statement statement) throws SQLException {
    try {
      ResultSet createStageResult = statement.executeQuery("CREATE OR REPLACE STAGE " + stageName);
      if (createStageResult.next()) {
        boolean result = createStageResult.getString(1).equals("Stage area " + stageName.toUpperCase() + " successfully created.");
        logMetric("cloudprober_driver_java_create_stage", result ? Status.SUCCESS : Status.FAILURE);
      } else {
        logMetric("cloudprober_driver_java_create_stage", Status.FAILURE);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      logMetric("cloudprober_driver_java_create_stage", Status.FAILURE);
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
    System.out.println(metricName + "{java_version=\"" + javaVersion + "\", driver_version=\"" + driverVersion + "\"} " + status.getCode());
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