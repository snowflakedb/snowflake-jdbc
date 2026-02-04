package net.snowflake;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class FatJarTestApp {
  public static void main(String[] args) throws Exception {
    try(Connection connection = getConnection(args); Statement stmt = connection.createStatement()) {
      System.out.println("RUNNING SELECT 1");
      ResultSet resultSet = stmt.executeQuery("SELECT 1");
      if (!resultSet.next()) {
        throw new RuntimeException("No data found");
      }
      if (resultSet.getInt(1) != 1) {
        throw new RuntimeException("Wrong data found: " + resultSet.getInt(1));
      }

      System.out.println("CREATING A STAGE");
      stmt.execute("CREATE OR REPLACE TEMPORARY STAGE fat_jar_stage");
      System.out.println("PUTTING A FILE");
      stmt.execute("PUT file://" + getTestFilePath() + " @fat_jar_stage");
    }
  }

  private static String getTestFilePath() throws URISyntaxException {
    if (new File("/tmp/test.csv").exists()) {
      return "/tmp/test.csv";
    } else {
      return new File(FatJarTestApp.class.getClassLoader().getResource("test.csv").toURI()).getAbsolutePath();
    }
  }

  private static Connection getConnection(String[] args) throws ClassNotFoundException, SQLException {
    Properties properties = new Properties();
    properties.put("user", getSfEnv("USER"));
    String authenticator = getSfEnv("AUTHENTICATOR");
    if ("SNOWFLAKE_JWT".equals(authenticator)) {
      String privateKeyFile = getSfEnv("PRIVATE_KEY_FILE");
      properties.put("private_key_file", privateKeyFile);
      properties.put("authenticator", "SNOWFLAKE_JWT");
    } else {
      String password = getSfEnv("PASSWORD");
      properties.put("password", password);
    }
    properties.put("role", getSfEnv("ROLE"));
    properties.put("account", getSfEnv("ACCOUNT"));
    properties.put("db", getSfEnv("DATABASE"));
    properties.put("schema", getSfEnv("SCHEMA"));
    properties.put("warehouse", getSfEnv("WAREHOUSE"));
    properties.put("ssl", true);
    String uri = "jdbc:snowflake://" + getSfEnv("ACCOUNT") + ".snowflakecomputing.com";
    if (Arrays.asList(args).contains("enableDiagnostics")) {
      uri += "?ENABLE_DIAGNOSTICS=true&DIAGNOSTICS_ALLOWLIST_FILE=/allowlist.json";
    }
    if (Arrays.asList(args).contains("useProxy")) {
      properties.put("useProxy", "true");
      properties.put("proxyHost", "localhost");
      properties.put("proxyPort", "8080");
    }
    Class.forName("net.snowflake.client.api.driver.SnowflakeDriver");
    return DriverManager.getConnection(uri, properties);
  }

  private static String getSfEnv(String param) {
    return System.getenv("SNOWFLAKE_TEST_" + param);
  }
}
