package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Stream interface tests. Snowflake JDBC specific API */
@Tag(TestTags.OTHERS)
public class StreamIT extends BaseJDBCTest {
  /**
   * Test Upload Stream
   *
   * @throws Throwable if any error occurs.
   */
  @Test
  public void testUploadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
        outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        // upload the data to user stage under testUploadStream with name hello.txt
        connection
            .unwrap(SnowflakeConnection.class)
            .uploadStream(
                "~", DEST_PREFIX, outputStream.asByteSource().openStream(), "hello.txt", false);

        // select from the file to make sure the data is uploaded
        try (ResultSet rset = statement.executeQuery("SELECT $1 FROM @~/" + DEST_PREFIX)) {
          String ret = null;

          while (rset.next()) {
            ret = rset.getString(1);
          }
          assertEquals("hello", ret, "Unexpected string value: " + ret + " expect: hello");
        }
      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
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
  @DontRunOnGithubActions
  public void testDownloadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    List<String> supportedAccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAccounts) {
      try (Connection connection = getConnection(accountName);
          Statement statement = connection.createStatement()) {
        try {
          try (ResultSet rset =
              statement.executeQuery(
                  "PUT file://"
                      + getFullPathFileInResource(TEST_DATA_FILE)
                      + " @~/"
                      + DEST_PREFIX)) {
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
          }
        } finally {
          statement.execute("rm @~/" + DEST_PREFIX);
        }
      }
    }
  }

  @Test
  public void testCompressAndUploadStream() throws Throwable {
    final String DEST_PREFIX = TEST_UUID + "/" + "testCompressAndUploadStream";
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
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
        try (ResultSet rset = statement.executeQuery("SELECT $1 FROM @~/" + DEST_PREFIX)) {

          String ret = null;
          while (rset.next()) {
            ret = rset.getString(1);
          }
          assertEquals("hello", ret, "Unexpected string value: " + ret + " expect: hello");
        }

      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
    }
  }
}
