package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public final class SessionVariablesIT extends AbstractDriverIT {
  @Test
  public void testSettingSessionVariablesInConnectionProperties() throws SQLException {
    Properties properties = new Properties();
    properties.put("$var1", "some example 1");
    properties.put("$var2", "2");
    properties.put("var3", "some example 3");

    try (Connection con = getConnection(properties);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show variables")) {
      Map<String, String> variablesInSession = new HashMap<>();
      while (resultSet.next()) {
        variablesInSession.put(resultSet.getString("name"), resultSet.getString("value"));
      }

      assertEquals("some example 1", variablesInSession.get("VAR1"));
      assertEquals("2", variablesInSession.get("VAR2"));
      assertNull(variablesInSession.get("VAR3"));
    }
  }
}
