package net.snowflake;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.logging.LogManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class FatJarTestApp {

  private static final String LOGGER_IMPL_PROPERTY = "net.snowflake.jdbc.loggerImpl";
  private static final String SLF4J_LOGGER_CLASS = "net.snowflake.client.log.SLF4JLogger";

  private static final String LOGGER_IMPL = System.getProperty(LOGGER_IMPL_PROPERTY);

  private static final String[] CLOUD_SDK_LOGGER_PATTERNS = {
    "net.snowflake.client.jdbc.internal.software.amazon",
    "net.snowflake.client.jdbc.internal.google",
    "net.snowflake.client.jdbc.internal.azure"
  };

  private static final File logFile =
      new File(System.getProperty("java.io.tmpdir"), "fat-jar-test.log");

  // Static initializer to load FIPS configuration if available
  static {
    try {
      // This will only succeed when FipsInitializer.class is on the classpath
      // (i.e., when compiled with the fips profile)
      Class<?> fipsInitializer = Class.forName("net.snowflake.FipsInitializer");
      fipsInitializer.getMethod("ensureInitialized").invoke(null);
      System.out.println("[INFO] Running in FIPS mode");
    } catch (ClassNotFoundException e) {
      // FIPS not available - normal mode
      System.out.println("[INFO] Running in normal (non-FIPS) mode");
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize FIPS mode", e);
    }
  }

  public static void main(String[] args) throws Exception {
    setupLogging();
    runQueries(args);
    verifyLogs();
  }

  private static void runQueries(String[] args) throws Exception {
    try (Connection connection = getConnection(args); Statement stmt = connection.createStatement()) {
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

  private static void setupLogging() throws Exception {
    if (SLF4J_LOGGER_CLASS.equals(LOGGER_IMPL)) {
      System.setProperty("fatjar.logfile", logFile.getAbsolutePath());
      System.out.println("[INFO] SLF4J logging to: " + logFile.getAbsolutePath());
    } else {
      try (InputStream is = FatJarTestApp.class.getResourceAsStream("/logging.properties")) {
        LogManager.getLogManager().readConfiguration(is);
      }
      System.out.println("[INFO] JUL logging to: " + logFile.getAbsolutePath());
    }
  }

  private static void verifyLogs() throws Exception {
    String logOutput = new String(Files.readAllBytes(logFile.toPath()));
    String mode = SLF4J_LOGGER_CLASS.equals(LOGGER_IMPL) ? "SLF4J" : "JUL";
    System.out.println("[INFO] Verifying " + mode + " log output (" + logOutput.length() + " chars)");
    String logsPrelude = logOutput.substring(0, Math.min(2000, logOutput.length()));

    boolean hasOpeningSession = logOutput.contains("Opening session");
    if (!hasOpeningSession) {
      System.err.println("[FAIL] Log output does not contain 'Opening session' (expected from SFSession)");
      System.err.println("[DEBUG] First 2000 chars of log output:");
      System.err.println(logsPrelude);
      System.exit(1);
    }
    System.out.println("[PASS] Found 'Opening session' in " + mode + " log output");

    boolean hasCloudSdkLog = false;
    for (String pattern : CLOUD_SDK_LOGGER_PATTERNS) {
      if (logOutput.contains(pattern)) {
        hasCloudSdkLog = true;
        System.out.println("[PASS] Found cloud SDK logger: " + pattern);
        break;
      }
    }
    if (!hasCloudSdkLog) {
      System.err.println("[FAIL] Log output does not contain any cloud SDK logger name");
      System.err.println("[FAIL] Expected one of: " + Arrays.toString(CLOUD_SDK_LOGGER_PATTERNS));
      System.err.println("[DEBUG] First 2000 chars of log output:");
      System.err.println(logsPrelude);
      System.exit(1);
    }

    System.out.println("[PASS] All " + mode + " logging verifications passed");
  }

  private static String getTestFilePath() throws URISyntaxException {
    if (new File("/tmp/test.csv").exists()) {
      return "/tmp/test.csv";
    } else {
      return new File(FatJarTestApp.class.getClassLoader().getResource("test.csv").toURI()).getAbsolutePath();
    }
  }

  private static Connection getConnection(String[] args) throws Exception {
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
