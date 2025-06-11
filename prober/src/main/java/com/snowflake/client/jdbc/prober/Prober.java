package com.snowflake.client.jdbc.prober;

import net.snowflake.client.jdbc.SnowflakeConnection;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
  private static final String stageName = "test_stage";
  private static final String stageFilePath = "test_file.txt";
  private static final String tableName = "test_table";

  public static void main(String[] args) throws IOException, SQLException {
    disableLogging();
    Map<String, String> arguments = parseArguments(args);

    String url = "jdbc:snowflake://" + arguments.get("account") + "." + arguments.get("host");
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }

    testLogin(url, props);
    testPutFetchGet(url, props);
  }

  private static void testPutFetchGet(String url, Properties properties) {
    try (Connection connection = DriverManager.getConnection(url, properties);
         Statement statement = connection.createStatement()) {
      SnowflakeConnection sfConnection = connection.unwrap(SnowflakeConnection.class);
      List<String> csv = generateCsv(1000);
      String csvFile = csv.stream().collect(Collectors.joining(System.lineSeparator()));
      createDataStage(statement);
      createDataTable(statement);

      uploadFile(sfConnection, csvFile);
      loadFileIntoTable(statement);
      fetchAndVerifyRows(statement);
      downloadFile(sfConnection);
      compareFetchedDataAndFile(statement, csv);
    } catch (SQLException e) {
      System.err.println(e.getMessage());
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
        System.out.println("{\"data_integrity_check\": false}");
        return;
      }
      int dbId = resultSet.getInt(1);
      String dbName = resultSet.getString(2);
      String dbEmail = resultSet.getString(3);

      boolean idMatch = (dbId == listId);
      boolean nameMatch = dbName.equals(listName);
      boolean emailMatch = dbEmail.equals(listEmail);
      if (!(idMatch && nameMatch && emailMatch)) {
        System.out.println("{\"data_integrity_check\": false}");
        return;
      }
    }
    System.out.println("{\"data_integrity_check\": true}");
  }

  private static String downloadFile(SnowflakeConnection sfConnection) throws SQLException {
    InputStream downloadStream = sfConnection.downloadStream("@" + stageName, stageFilePath, false);
    BufferedReader reader = new BufferedReader(new InputStreamReader(downloadStream, StandardCharsets.UTF_8));
    List<String> lines = reader.lines().collect(Collectors.toList());
    if (lines.size() == 1001) {
      System.out.println("{\"GET_operation\": true}");
    } else {
      System.out.println("{\"GET_operation\": false}");
    }
    return lines.stream().collect(Collectors.joining(System.lineSeparator()));
  }

  private static void fetchAndVerifyRows(Statement statement) throws SQLException {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
    if (resultSet.next()) {
      int rowCount = resultSet.getInt(1);
      boolean success = rowCount == 1000;
      System.out.println("{\"data_transferred_completely\": " + success + "}");
    } else {
      System.out.println("{\"data_transferred_completely\": false}");
    }
  }

  private static void loadFileIntoTable(Statement statement) throws SQLException {
    try {
      statement.executeQuery("copy into " + tableName + " from @" + stageName + "/" + stageFilePath + " FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);");
      System.out.println("{\"copied_data_from_stage_into_table\": true}");
    } catch (SQLException e) {
      System.err.println("Error during copy into table: " + e.getMessage());
      System.out.println("{\"copied_data_from_stage_into_table\": false}");
    }
  }

  private static void uploadFile(SnowflakeConnection sfConnection, String fileContent) throws SQLException {
    try {
      sfConnection.uploadStream("@" + stageName, "", new ByteArrayInputStream(fileContent.getBytes()), stageFilePath, false);
      System.out.println("{\"PUT_operation\": true}");
    } catch (SQLException e) {
      System.err.println("Error during file upload: " + e.getMessage());
      System.out.println("{\"PUT_operation\": false}");
    }
  }

  private static void createDataTable(Statement statement) throws SQLException {
    ResultSet resultSet = statement.executeQuery("CREATE OR REPLACE TABLE " + tableName + " (id int, name text, email text)");
    if (resultSet.next()) {
      boolean result = resultSet.getString(1).equals("Table " + tableName.toUpperCase() + " successfully created.");
      System.out.println("{\"created_table\": " + result + "}");
    }
  }

  private static void createDataStage(Statement statement) throws SQLException {
    ResultSet createStageResult = statement.executeQuery("CREATE OR REPLACE TEMP STAGE " + stageName);
    if (createStageResult.next()) {
      boolean success = createStageResult.getString(1).equals("Stage area " + stageName.toUpperCase() + " successfully created.");
      System.out.println("{\"created_stage\": " + success + "}");
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
    }
    System.out.println("{\"success_login\": " + success + "}");
  }

  private static void disableLogging() throws IOException {
    String loggingPropertiesString = ".level=OFF";
    try (InputStream propertiesStream = new ByteArrayInputStream(
        loggingPropertiesString.getBytes(StandardCharsets.UTF_8)
    )) {
      LogManager.getLogManager().readConfiguration(propertiesStream);
    }
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
}