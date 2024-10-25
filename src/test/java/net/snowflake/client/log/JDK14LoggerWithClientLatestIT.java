package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnWin;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryOthers.class)
public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  String homePath = systemGetProperty("user.home");

  @Test
  public void testJDK14LoggingWithClientConfig() throws IOException {
    File configFile = tmpFolder.newFile("config.json");
    Path configFilePath = configFile.toPath();
    File logFolder = tmpFolder.newFolder("logs");
    Path logFolderPath = logFolder.toPath();
    String configJson =
        "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"" + logFolderPath + "\"}}";
    try {
      Files.write(configFilePath, configJson.getBytes());
      Properties properties = new Properties();
      properties.put("client_config_file", configFilePath.toString());
      try (Connection connection = getConnection(properties);
          Statement statement = connection.createStatement()) {
        statement.executeQuery("select 1");

        File file = new File(Paths.get(logFolderPath.toString(), "jdbc").toString());
        System.out.println("Expected: " + file.toPath());
        System.out.print("Actual:   ");
        for (File f : logFolder.listFiles()) {
          System.out.println(f.toPath());
        }
        assertTrue(file.exists());
      }
    } catch (IOException e) {
      fail("testJDK14LoggingWithClientConfig failed");
    } catch (SQLException e) {
      fail("testJDK14LoggingWithClientConfig failed");
    }
  }

  @Test(expected = SQLException.class)
  public void testJDK14LoggingWithClientConfigInvalidConfigFilePath() throws SQLException {
    Path configFilePath = Paths.get("invalid.json");
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties)) {
      connection.createStatement().executeQuery("select 1");
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnWin.class)
  public void testJDK14LoggingWithClientConfigPermissionError() throws IOException {
    File configFile = tmpFolder.newFile("config.json");
    Path configFilePath = configFile.toPath();
    File directory = tmpFolder.newFolder("logs");
    Path directoryPath = directory.toPath();
    String configJson =
        "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"" + directoryPath + "\"}}";
    HashSet<PosixFilePermission> perms = new HashSet<>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    System.out.println("Perms before: " + Files.getPosixFilePermissions(directoryPath));
    Files.setPosixFilePermissions(directoryPath, perms);
    System.out.println("Perms after: " + Files.getPosixFilePermissions(directoryPath));
    Files.createDirectories(Paths.get(directoryPath.toString(), "jdbc"));
    System.out.println("Created subfolder without permissions");

    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    Exception e = assertThrows(SQLException.class, () -> getConnection(properties));
    System.out.println(e.getMessage());
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
    File configFile = tmpFolder.newFile("config.json");
    Path configFilePath = configFile.toPath();
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";
    Path home = tmpFolder.getRoot().toPath();
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
  public void testJDK14LoggingWithMissingLogPathNoHomeDirClientConfig() throws Exception {
    System.clearProperty("user.home");

    File configFile = tmpFolder.newFile("config.json");
    Path configFilePath = configFile.toPath();
    String configJson = "{\"common\":{\"log_level\":\"debug\"}}";
    Files.write(configFilePath, configJson.getBytes());
    Properties properties = new Properties();
    properties.put("client_config_file", configFilePath.toString());
    try (Connection connection = getConnection(properties)) {

      fail("testJDK14LoggingWithMissingLogPathNoHomeDirClientConfig failed");
    } catch (SnowflakeSQLLoggedException e) {
      // Succeed
    } finally {
      System.setProperty("user.home", homePath);
      Files.deleteIfExists(configFilePath);
    }
  }
}
