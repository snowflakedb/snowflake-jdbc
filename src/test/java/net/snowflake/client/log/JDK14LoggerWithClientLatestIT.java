package net.snowflake.client.log;

import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.fail;

public class JDK14LoggerWithClientLatestIT extends AbstractDriverIT {

    @Test
    public void testJDK14LoggingWithClientConfig(){
        Path configFilePath = Paths.get("config.json");
        String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"/logs\"}}";
        try {
            Files.write(configFilePath, configJson.getBytes());
            Properties properties = new Properties();
            properties.put("clientConfigFile", configFilePath.toString());
            Connection connection = getConnection(properties);
            connection.createStatement().executeQuery("select 1");

            File file = new File("/logs/jdbc/");


        }catch (IOException e) {
            fail("testJDK14LoggingWithClientConfig failed");
        } catch (SQLException e) {
            fail("testJDK14LoggingWithClientConfig failed");
        }
    }
}
