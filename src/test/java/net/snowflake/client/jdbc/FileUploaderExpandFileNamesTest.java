/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.util.Set;

import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

/**
 * Tests for SnowflakeFileTransferAgent.expandFileNames
 */
@Category(TestCategoryOthers.class)
public class FileUploaderExpandFileNamesTest
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
