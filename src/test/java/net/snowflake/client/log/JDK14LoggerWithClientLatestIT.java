package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnWindows;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.OTHERS)
public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

  @TempDir public File tmpFolder;
  String homePath = systemGetProperty("user.home");
  private static Level originalLevel;

  @BeforeAll
  static void saveLevel() {
    originalLevel = JDK14Logger.getLevel();
  }

  @AfterAll
  static void restoreLevel() {
    JDK14Logger.setLevel(originalLevel);
  }

  @Test
  @Disabled
  public void testJDK14LoggingWithClientConfig() throws IOException, SQLException {
    File configFile = new File(tmpFolder, "config.json");
    configFile.createNewFile();
    Path configFilePath = configFile.toPath();
    File logFolder = new File(tmpFolder, "logs");
    logFolder.createNewFile();
    Path logFolderPath = logFolder.toPath();
    String configJson =
        "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"" + logFolderPath + "\"}}";

    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select 1");

      File file = new File(Paths.get(logFolderPath.toString(), "jdbc").toString());
      assertTrue(file.exists());
    }
  }

  @Test
  public void testJDK14LoggingWithClientConfigInvalidConfigFilePath() {
    Path configFilePath = Paths.get("invalid.json");
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    assertThrows(
        SnowflakeSQLException.class,
        () -> {
          try (Connection connection = getConnection(properties)) {
            connection.createStatement().executeQuery("select 1");
          }
        });
  }

  @Test
  @Disabled
  @DontRunOnWindows
  public void testJDK14LoggingWithClientConfigPermissionError() throws IOException {
    File configFile = new File(tmpFolder, "config.json");
    configFile.createNewFile();
    Path configFilePath = configFile.toPath();
    File logFolder = new File(tmpFolder, "logs");
    logFolder.createNewFile();
    Path logFolderPath = logFolder.toPath();
    String configJson =
        "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"" + logFolderPath + "\"}}";
    HashSet<PosixFilePermission> perms = new HashSet<>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setPosixFilePermissions(logFolderPath, perms);

    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    assertThrows(SQLException.class, () -> getConnection(properties));
  }

  @Test
  public void testJDK14LoggerWithBracesInMessage() {
    JDK14Logger logger = new JDK14Logger(JDK14LoggerWithClientLatestIT.class.getName());
    JDK14Logger.setLevel(Level.FINE);
    logger.debug("Returning column: 12: a: Group b) Hi {Hello World War} cant wait");
    JDK14Logger.setLevel(Level.OFF);
  }

  @Test
  public void testJDK14LoggerWithQuotesInMessage() {
    JDK14Logger logger = new JDK14Logger(JDK14LoggerWithClientLatestIT.class.getName());
    JDK14Logger.setLevel(Level.FINE);
    logger.debug("Returning column: 12: a: Group b) Hi {Hello 'World' War} cant wait");
    JDK14Logger.setLevel(Level.OFF);
  }

  @Test
  @Disabled
  public void testJDK14LoggingWithMissingLogPathClientConfig() throws Exception {
    File configFile = new File(tmpFolder, "config.json");
    configFile.createNewFile();
    Path configFilePath = configFile.toPath();
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";
    Path home = tmpFolder.toPath();
    System.setProperty("user.home", home.toString());

    Path homeLogPath = Paths.get(home.toString(), "jdbc");
    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery("select 1");

        File file = new File(homeLogPath.toString());
        assertTrue(file.exists());

      } finally {
        Files.deleteIfExists(configFilePath);
        FileUtils.deleteDirectory(new File(homeLogPath.toString()));
      }
    } finally {
      System.setProperty("user.home", homePath);
    }
  }

  @Test
  @Disabled
  public void testJDK14LoggingWithMissingLogPathNoHomeDirClientConfig() throws Exception {
    System.clearProperty("user.home");

    File configFile = new File(tmpFolder, "config.json");
    Path configFilePath = configFile.toPath();
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";
    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try {
      assertThrows(SnowflakeSQLLoggedException.class, () -> getConnection(properties));
    } finally {
      System.setProperty("user.home", homePath);
      Files.deleteIfExists(configFilePath);
    }
  }
}
