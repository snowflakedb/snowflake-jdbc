package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestTags;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.OTHERS)
public class PutFileWithSpaceIncludedIT extends BaseJDBCTest {
  @TempDir private File tmpFolder;

  /** Test PUT command to send a data file, which file name contains a space. */
  @Test
  @Disabled
  public void putFileWithSpaceIncluded() throws Exception {
    String AWS_SECRET_KEY = TestUtil.systemGetEnv("AWS_SECRET_ACCESS_KEY");
    String AWS_KEY_ID = TestUtil.systemGetEnv("AWS_ACCESS_KEY_ID");
    String SF_AWS_USER_BUCKET = TestUtil.systemGetEnv("SF_AWS_USER_BUCKET");
    if (SF_AWS_USER_BUCKET == null) {
      String userName = TestUtil.systemGetEnv("USERNAME");
      assertNotNull(userName);
      SF_AWS_USER_BUCKET = "sfc-dev1-regression/" + userName + "/snow-13400";
    }

    assertNotNull(AWS_SECRET_KEY);
    assertNotNull(AWS_KEY_ID);

    File dataFolder = new File(tmpFolder, "data");
    dataFolder.mkdirs();
    String tarFile = getFullPathFileInResource("snow-13400.tar");
    FileInputStream fis = new FileInputStream(tarFile);
    TarArchiveInputStream tis = new TarArchiveInputStream(fis);
    TarArchiveEntry tarEntry;
    while ((tarEntry = tis.getNextTarEntry()) != null) {
      File outputFile = new File(dataFolder, tarEntry.getName());
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        IOUtils.copy(tis, fos);
      }
    }

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute(
            "create or replace stage snow13400 url='s3://"
                + SF_AWS_USER_BUCKET
                + "/snow13400'"
                + "credentials=(AWS_KEY_ID='"
                + AWS_KEY_ID
                + "' AWS_SECRET_KEY='"
                + AWS_SECRET_KEY
                + "')");

        try (ResultSet resultSet =
            statement.executeQuery(
                "put file://"
                    + dataFolder.getCanonicalPath()
                    + "/* @snow13400 auto_compress=false")) {
          int cnt = 0;
          while (resultSet.next()) {
            cnt++;
          }
          assertEquals(cnt, 1);
        }
        statement.execute("create or replace table snow13400(a string)");
        statement.execute("copy into snow13400 from @snow13400");
        try (ResultSet resultSet = con.createStatement().executeQuery("select * from snow13400")) {
          int cnt = 0;
          String output = null;
          while (resultSet.next()) {
            output = resultSet.getString(1);
            cnt++;
          }
          assertEquals(cnt, 1);
          assertEquals(output, "hello");
        }
      } finally {
        statement.execute("rm @snow13400");
        statement.execute("drop stage if exists snow13400");
        statement.execute("drop table if exists snow13400");
      }
    }
  }
}
