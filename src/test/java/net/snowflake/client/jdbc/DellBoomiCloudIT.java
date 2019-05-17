package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.Policy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A simple run on fetch result under boomi cloud environment's policy file
 */
public class DellBoomiCloudIT extends AbstractDriverIT
{
  @Before
  public void setup()
  {
    File file =
        new File(DellBoomiCloudIT.class.getResource("boomi.policy").getFile());

    System.setProperty("java.security.policy", file.getAbsolutePath());
    Policy.getPolicy().refresh();
    System.setSecurityManager(new SecurityManager());
  }

  @Test
  public void testSelectLargeResultSet() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select seq4() from table" +
                                                 "(generator" +
                                                 "(rowcount=>10000))");

    while (resultSet.next())
    {
      resultSet.getString(1);
    }

    resultSet.close();
    statement.close();
    connection.close();
  }
}
