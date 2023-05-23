package com.snowflake.client.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

/**
 * Example program that runs against reproducible table
 */
public class ExampleOnlyAndDeleteMeAtEnd {

  public static void main(String[] args) throws Exception {

    // get connection
    System.out.println("Create JDBC connection");
    Connection connection = getConnection();
    System.out.println("Done creating JDBC connection\n");

    // create statement
    System.out.println("Create JDBC statement");
    Statement statement = connection.createStatement();
    System.out.println("Done creating JDBC statement\n");

    // query the data
    System.out.println("Query demo");
    ResultSet resultSet = statement.executeQuery("select ID, LIEF_E_MAIL_ADR, HEX_ENCODE(LIEF_E_MAIL_ADR) AS HX, LENGTH(LIEF_E_MAIL_ADR), LENGTH(HX) from BEDROCK_DB.BEDROCK_SCHEMA.WALEED order by id asc");

    System.out.println("Metadata:");
    System.out.println("================================");

    // fetch metadata
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    System.out.println("Number of columns=" + resultSetMetaData.getColumnCount());
    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
      System.out.println(
          "Column " + colIdx + ": type=" + resultSetMetaData.getColumnTypeName(colIdx + 1));
    }

    // fetch data
    System.out.println("\nData discrepancies:");
    System.out.println("================================");
    int rowIdx = 0;
    int column1;
    String column2;
    String column3;
    long column3ComputedLen;
    long column3ServerLength;
    long column2ServerLength;
    String java_hex;

    while (resultSet.next()) {
      column1 = resultSet.getInt(1);
      column2 = resultSet.getString(2);
      column3 = resultSet.getString(3);
      column3ComputedLen = column3.length();
      // Space out the hex every 2 characters to make it easier to read visually
      column3 = column3.replaceAll("..", "$0 ");
      java_hex = stringToHex(column2);
      column2ServerLength = resultSet.getLong(4);
      column3ServerLength = resultSet.getLong(5);
      rowIdx++;
      if (!java_hex.trim().equals(column3.trim()) || column2.length() != column2ServerLength || column3ComputedLen != column3ServerLength) {
        System.out.println("row " + rowIdx + ", column 1 (ID): "
                + column1 + ", column 2 (LIEF_E_MAIL_ADR): "
                + " length matches? " + (column2.length() == column2ServerLength)
                + " hex length matches? " + (column3ComputedLen == column3ServerLength) + " "
                + column2 + ", column 3 (HEX_ENCODE(LIEF_E_MAIL_ADR)): \n" + column3);
        System.out.println(java_hex);
      }
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  public static String stringToHex(String input){
    byte[] byteArray = input.getBytes(StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder();
    char[] hexBytes = new char[2];
    for(int i=0; i < byteArray.length; i++){
      hexBytes[0] = Character.forDigit((byteArray[i] >> 4) & 0xF, 16);
      hexBytes[1] = Character.forDigit((byteArray[i] & 0xF), 16);
      sb.append(hexBytes);
      sb.append(" "); // Space every two characters to make it easier to read visually
    }
    return sb.toString().toUpperCase();
  }

  private static Connection getConnection() throws SQLException {

    // build connection properties
    Properties properties = new Properties();
    properties.put("user", "USERNAME"); // replace "" with your user name
    properties.put("password", "PASSWORD"); // replace "" with your password
    properties.put("role", "ACCOUNTADMIN"); // replace "" with your password
    properties.put("warehouse", "BEDROCK_WH"); // replace "" with target warehouse name
    properties.put("db", "BEDROCK_DB"); // replace "" with target database name
    properties.put("schema", "BEDROCK_SCHEMA"); // replace "" with target schema name
    properties.put("authenticator", "snowflake");
    properties.put("jdbc_query_result_format", "json");
    properties.put("tracing", "all");
    // properties.put("USE_CACHED_RESULT", "false");
    // properties.put("CLIENT_ENABLE_CONSERVATIiVE_MEMORY_USAGE","FALSE");

    // Replace <account_identifier> with your account identifier. See
    // https://docs.snowflake.com/en/user-guide/admin-account-identifier.html
    // for details.
    String connectStr = "jdbc:snowflake://sfcsupport2.snowflakecomputing.com";
    return DriverManager.getConnection(connectStr, properties);
  }
}

