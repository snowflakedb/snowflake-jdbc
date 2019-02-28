/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

/**
 * @author jhuang
 */
public class SnowflakeFileUploaderTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testProcessFileNames() throws Exception
  {
    folder.newFile("TestFileA");
    folder.newFile("TestFileB");

    String folderName = folder.getRoot().getCanonicalPath();
    System.setProperty("user.dir", folderName);
    System.setProperty("user.home", folderName);

    String[] locations =
        {
            folderName + "/Tes*Fil*A",
            folderName + "/TestFil?B",
            "~/TestFileC", "TestFileD"
        };

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations);

    assertTrue(files.contains(folderName + "/TestFileA"));
    assertTrue(files.contains(folderName + "/TestFileB"));
    assertTrue(files.contains(folderName + "/TestFileC"));
    assertTrue(files.contains(folderName + "/TestFileD"));
  }
}
