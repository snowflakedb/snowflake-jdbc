package net.snowflake.client.log;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import net.snowflake.client.AbstractDriverIT;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

  @Test
  public void testJDK14LoggingWithClientConfig() {
    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
    try {
      Files.write(configFilePath, configJson.getBytes());
      Properties properties = new Properties();
      properties.put("client_config_file", configFilePath.toString());
      Connection connection = getConnection(properties);
      connection.createStatement().executeQuery("select 1");

      File file = new File("logs/jdbc/");
      assertTrue(file.exists());

      Files.deleteIfExists(configFilePath);
      FileUtils.deleteDirectory(new File("logs"));
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
    Connection connection = getConnection(properties);
    connection.createStatement().executeQuery("select 1");
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
    assertThrows(SQLException.class, () -> getConnection(properties));

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
}
