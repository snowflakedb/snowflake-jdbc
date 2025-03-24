package net.snowflake.client.jdbc;

import java.io.File;
import java.security.Policy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** A simple run on fetch result under boomi cloud environment's policy file */
@Tag(TestTags.OTHERS)
public class DellBoomiCloudIT extends AbstractDriverIT {
  @BeforeEach
  public void setup() {
    File file = new File(DellBoomiCloudIT.class.getResource("boomi.policy").getFile());

    System.setProperty("java.security.policy", file.getAbsolutePath());
    Policy.getPolicy().refresh();
    System.setSecurityManager(new SecurityManager());
  }

  @Test
  @Disabled // TODO: SNOW-1805239
  public void testSelectLargeResultSet() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select seq4() from table" + "(generator" + "(rowcount=>10000))")) {

      while (resultSet.next()) {
        resultSet.getString(1);
      }
    }
  }
}
