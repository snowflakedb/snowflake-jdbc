/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryOthers.class)
public class PutFileWithSpaceIncluded extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /** Test PUT command to send a data file, which file name contains a space. */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void putFileWithSpaceIncluded() throws Exception {
    String AWS_SECRET_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
    String AWS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
    String SF_AWS_USER_BUCKET = System.getenv("SF_AWS_USER_BUCKET");
    if (SF_AWS_USER_BUCKET == null) {
      String userName = System.getenv("USERNAME");
      assertNotNull(userName);
      SF_AWS_USER_BUCKET = "sfc-dev1-regression/" + userName + "/snow-13400";
    }

    assertNotNull(AWS_SECRET_KEY);
    assertNotNull(AWS_KEY_ID);

    File dataFolder = tmpFolder.newFolder();
    String tarFile = getFullPathFileInResource("snow-13400.tar");
    FileInputStream fis = new FileInputStream(tarFile);
    TarArchiveInputStream tis = new TarArchiveInputStream(fis);
    TarArchiveEntry tarEntry;
    while ((tarEntry = tis.getNextTarEntry()) != null) {
      File outputFile = new File(dataFolder, tarEntry.getName());
      FileOutputStream fos = new FileOutputStream(outputFile);
      IOUtils.copy(tis, fos);
      fos.close();
    }

    try (Connection con = getConnection()) {
      con.createStatement()
          .execute(
              "create or replace stage snow13400 url='s3://"
                  + SF_AWS_USER_BUCKET
                  + "/snow13400'"
                  + "credentials=(AWS_KEY_ID='"
                  + AWS_KEY_ID
                  + "' AWS_SECRET_KEY='"
                  + AWS_SECRET_KEY
                  + "')");

      {
        ResultSet resultSet =
            con.createStatement()
                .executeQuery(
                    "put file://"
                        + dataFolder.getCanonicalPath()
                        + "/* @snow13400 auto_compress=false");
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(cnt, 1);
      }
      con.createStatement().execute("create or replace table snow13400(a string)");
      con.createStatement().execute("copy into snow13400 from @snow13400");
      {
        ResultSet resultSet = con.createStatement().executeQuery("select * from snow13400");
        int cnt = 0;
        String output = null;
        while (resultSet.next()) {
          output = resultSet.getString(1);
          cnt++;
        }
        assertEquals(cnt, 1);
        assertEquals(output, "hello");
      }
      con.createStatement().execute("rm @snow13400");
      con.createStatement().execute("drop stage if exists snow13400");
      con.createStatement().execute("drop table if exists snow13400");
    }
  }
}
