/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTestaccount;
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Stream interface tests. Snowflake JDBC specific API */
@Category(TestCategoryOthers.class)
public class StreamIT extends BaseJDBCTest {
  /**
   * Test Upload Stream
   *
   * @throws Throwable if any error occurs.
   */
  @Test
  public void testUploadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
      outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
      outputStream.flush();

      // upload the data to user stage under testUploadStream with name hello.txt
      connection
          .unwrap(SnowflakeConnection.class)
          .uploadStream(
              "~", DEST_PREFIX, outputStream.asByteSource().openStream(), "hello.txt", false);

      // select from the file to make sure the data is uploaded
      ResultSet rset = statement.executeQuery("SELECT $1 FROM @~/" + DEST_PREFIX);

      String ret = null;

      while (rset.next()) {
        ret = rset.getString(1);
      }
      rset.close();
      assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);
    } finally {
      if (statement != null) {
        statement.execute("rm @~/" + DEST_PREFIX);
        statement.close();
      }
      closeSQLObjects(statement, connection);
    }
  }

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

  /**
   * Test Download Stream
   *
   * <p>NOTE: this doesn't work on testaccount using the local FS.
   *
   * @throws Throwable if any error occurs.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTestaccount.class)
  public void testDownloadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();
      statement = connection.createStatement();
      ResultSet rset =
          statement.executeQuery(
              "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @~/" + DEST_PREFIX);
      assertTrue(rset.next());
      assertEquals("UPLOADED", rset.getString(7));

      InputStream out =
          connection
              .unwrap(SnowflakeConnection.class)
              .downloadStream("~", DEST_PREFIX + "/" + TEST_DATA_FILE + ".gz", true);
      StringWriter writer = new StringWriter();
      IOUtils.copy(out, writer, "UTF-8");
      String output = writer.toString();
      // the first 2 characters
      assertEquals("1|", output.substring(0, 2));

      // the number of lines
      String[] lines = output.split("\n");
      assertEquals(28, lines.length);
    } finally {
      if (statement != null) {
        statement.execute("rm @~/" + DEST_PREFIX);
        statement.close();
      }
      closeSQLObjects(statement, connection);
    }
  }

  @Test
  public void testCompressAndUploadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/" + "testCompressAndUploadStream";
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
      outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
      outputStream.flush();

      // upload the data to user stage under testCompressAndUploadStream
      // with name hello.txt
      // upload the data to user stage under testUploadStream with name hello.txt
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .uploadStream(
              "~", DEST_PREFIX, outputStream.asByteSource().openStream(), "hello.txt", true);

      // select from the file to make sure the data is uploaded
      ResultSet rset = statement.executeQuery("SELECT $1 FROM @~/" + DEST_PREFIX);

      String ret = null;
      while (rset.next()) {
        ret = rset.getString(1);
      }
      rset.close();
      assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);
    } finally {
      if (statement != null) {
        statement.execute("rm @~/" + DEST_PREFIX);
        statement.close();
      }
      closeSQLObjects(resultSet, statement, connection);
    }
  }
}
