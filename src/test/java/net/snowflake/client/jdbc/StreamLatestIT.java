/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Stream API tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to StreamIT so that both the latest and oldest
 * supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class StreamLatestIT extends BaseJDBCTest {
  /**
   * Test Upload Stream with atypical stage names
   *
   * @throws Throwable if any error occurs.
   */
  @Test
  public void testUnusualStageName() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    try {
      statement.execute("CREATE or replace TABLE \"ice cream (nice)\" (types STRING)");

      FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
      outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
      outputStream.flush();

      // upload the data to user stage under testUploadStream with name hello.txt
      connection
          .unwrap(SnowflakeConnection.class)
          .uploadStream(
              "'@%\"ice cream (nice)\"'",
              null, outputStream.asByteSource().openStream(), "hello.txt", false);

      // select from the file to make sure the data is uploaded
      ResultSet rset = statement.executeQuery("SELECT $1 FROM '@%\"ice cream (nice)\"/'");

      String ret = null;

      while (rset.next()) {
        ret = rset.getString(1);
      }
      rset.close();
      assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);

      statement.execute("CREATE or replace TABLE \"ice cream (nice)\" (types STRING)");

      // upload the data to user stage under testUploadStream with name hello.txt
      connection
          .unwrap(SnowflakeConnection.class)
          .uploadStream(
              "$$@%\"ice cream (nice)\"$$",
              null, outputStream.asByteSource().openStream(), "hello.txt", false);

      // select from the file to make sure the data is uploaded
      rset = statement.executeQuery("SELECT $1 FROM $$@%\"ice cream (nice)\"/$$");

      ret = null;

      while (rset.next()) {
        ret = rset.getString(1);
      }
      rset.close();
      assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);

    } finally {
      statement.execute("DROP TABLE IF EXISTS \"ice cream (nice)\"");
      statement.close();
      connection.close();
    }
  }
}
