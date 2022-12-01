/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.*;
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
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public class PutUnescapeBackslashIT extends AbstractDriverIT {
  @BeforeClass
  public static void setUpClass() throws Exception {}

  /**
   * Test PUT command for a file name including backslashes. The file name should be unescaped to
   * match the name. SNOW-25974
   */
  @Test
  public void testPutFileUnescapeBackslashes() throws Exception {
    String remoteSubDir = "testPut";
    String testDataFileName = "testdata.txt";

    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    Writer writer = null;
    Path topDataDir = null;
    try {
      topDataDir = Files.createTempDirectory("testPutFileUnescapeBackslashes");
      topDataDir.toFile().deleteOnExit();

      // create sub directory where the name includes spaces and
      // backslashes
      Path subDir = Files.createDirectories(Paths.get(topDataDir.toString(), "test dir\\\\3"));

      // create a test data
      File dataFile = new File(subDir.toFile(), testDataFileName);
      writer =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(dataFile.getCanonicalPath()), "UTF-8"));
      writer.write("1,test1");
      writer.close();

      // run PUT command
      connection = getConnection();
      statement = connection.createStatement();
      String sql =
          String.format("PUT 'file://%s' @~/%s/", dataFile.getCanonicalPath(), remoteSubDir);

      // Escape backslaches. This must be done by the application.
      sql = sql.replaceAll("\\\\", "\\\\\\\\");
      statement.execute(sql);

      resultSet =
          connection.createStatement().executeQuery(String.format("LS @~/%s/", remoteSubDir));
      while (resultSet.next()) {
        assertThat(
            "File name doesn't match",
            resultSet.getString(1),
            startsWith(String.format("%s/%s", remoteSubDir, testDataFileName)));
      }

    } finally {
      if (connection != null) {
        connection.createStatement().execute(String.format("RM @~/%s", remoteSubDir));
      }
      closeSQLObjects(resultSet, statement, connection);
      if (writer != null) {
        writer.close();
      }
      FileUtils.deleteDirectory(topDataDir.toFile());
    }
  }
}
