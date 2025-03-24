package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class PutUnescapeBackslashIT extends AbstractDriverIT {
  @BeforeAll
  public static void setUpClass() throws Exception {}

  /**
   * Test PUT command for a file name including backslashes. The file name should be unescaped to
   * match the name. SNOW-25974
   */
  @Test
  public void testPutFileUnescapeBackslashes() throws Exception {
    String remoteSubDir = "testPut";
    String testDataFileName = "testdata.txt";

    Path topDataDir = null;
    try {
      topDataDir = Files.createTempDirectory("testPutFileUnescapeBackslashes");
      topDataDir.toFile().deleteOnExit();

      // create sub directory where the name includes spaces and
      // backslashes
      Path subDir = Files.createDirectories(Paths.get(topDataDir.toString(), "test dir\\\\3"));

      // create a test data
      File dataFile = new File(subDir.toFile(), testDataFileName);
      try (Writer writer =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(dataFile.getCanonicalPath()), "UTF-8"))) {
        writer.write("1,test1");
      }
      // run PUT command
      try (Connection connection = getConnection();
          Statement statement = connection.createStatement()) {
        try {
          String sql =
              String.format("PUT 'file://%s' @~/%s/", dataFile.getCanonicalPath(), remoteSubDir);

          // Escape backslashes. This must be done by the application.
          sql = sql.replaceAll("\\\\", "\\\\\\\\");
          statement.execute(sql);

          try (ResultSet resultSet =
              connection.createStatement().executeQuery(String.format("LS @~/%s/", remoteSubDir))) {
            while (resultSet.next()) {
              assertThat(
                  "File name doesn't match",
                  resultSet.getString(1),
                  startsWith(String.format("%s/%s", remoteSubDir, testDataFileName)));
            }
          }
        } finally {
          statement.execute(String.format("RM @~/%s", remoteSubDir));
        }
      }
    } finally {
      FileUtils.deleteDirectory(topDataDir.toFile());
    }
  }
}
