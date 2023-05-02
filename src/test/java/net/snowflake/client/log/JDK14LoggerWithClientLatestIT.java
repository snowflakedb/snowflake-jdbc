package net.snowflake.client.log;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import org.junit.Test;

public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

  @Test
  public void testJDK14LoggingWithClientConfig() throws IOException {
    Path configFilePath = Paths.get("config.json");
    String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
    try {
      Files.write(configFilePath, configJson.getBytes());
      Properties properties = new Properties();
      properties.put("clientConfigFile", configFilePath.toString());
      Connection connection = getConnection(properties);
      connection.createStatement().executeQuery("select 1");

      File file = new File("logs/jdbc/");
      assertTrue(file.exists());
    } catch (IOException e) {
      fail("testJDK14LoggingWithClientConfig failed");
    } catch (SQLException e) {
      fail("testJDK14LoggingWithClientConfig failed");
    } finally {
      Files.deleteIfExists(configFilePath);
      Path outputPath = Paths.get("logs");
      Files.walk(outputPath)
          .sorted(java.util.Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
      Files.deleteIfExists(outputPath);
    }
  }
}
