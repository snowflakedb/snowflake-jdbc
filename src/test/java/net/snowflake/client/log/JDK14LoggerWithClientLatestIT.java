package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

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
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

  String homePath = systemGetProperty("user.home");

  @Test
  public void testJDK14LoggingWithClientConfig() {
    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
    try {
      Files.write(configFilePath, configJson.getBytes());
      Properties properties = new Properties();
      properties.put("client_config_file", configFilePath.toString());
      try (Connection connection = getConnection(properties);
          Statement statement = connection.createStatement()) {
        statement.executeQuery("select 1");

        File file = new File("logs/jdbc/");
        Assertions.assertTrue(file.exists());

        Files.deleteIfExists(configFilePath);
        FileUtils.deleteDirectory(new File("logs"));
      }
    } catch (IOException e) {
      Assertions.fail("testJDK14LoggingWithClientConfig failed");
    } catch (SQLException e) {
      Assertions.fail("testJDK14LoggingWithClientConfig failed");
    }
  }

  @Test
  public void testJDK14LoggingWithClientConfigInvalidConfigFilePath() {
    Assertions.assertThrows(
        SQLException.class,
        () -> {
          Path configFilePath = Paths.get("invalid.json");
          Properties properties = new Properties();
          properties.put("client_config_file", configFilePath.toString());
          try (Connection connection = getConnection(properties)) {
            connection.createStatement().executeQuery("select 1");
          }
        });
  }

  @Test
  public void testJDK14LoggingWithClientConfigPermissionError() throws IOException, SQLException {
    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
    Path directoryPath = Files.createDirectory(Paths.get("logs"));
    File directory = directoryPath.toFile();
    HashSet<PosixFilePermission> perms = new HashSet<>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setPosixFilePermissions(directoryPath, perms);

    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    Assertions.assertThrows(SQLException.class, () -> getConnection(properties));

    Files.delete(configFilePath);
    directory.delete();
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
  public void testJDK14LoggingWithMissingLogPathClientConfig() throws Exception {
    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";

    Path homeLogPath = Paths.get(homePath, "jdbc");
    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery("select 1");

        File file = new File(homeLogPath.toString());
        Assertions.assertTrue(file.exists());

      } finally {
        Files.deleteIfExists(configFilePath);
        FileUtils.deleteDirectory(new File(homeLogPath.toString()));
      }
    }
  }

  @Test
  public void testJDK14LoggingWithMissingLogPathNoHomeDirClientConfig() throws Exception {
    System.clearProperty("user.home");

    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";
    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {

      Assertions.fail("testJDK14LoggingWithMissingLogPathNoHomeDirClientConfig failed");
    } catch (SnowflakeSQLLoggedException e) {
      // Succeed
    } finally {
      System.setProperty("user.home", homePath);
      Files.deleteIfExists(configFilePath);
    }
  }
}
