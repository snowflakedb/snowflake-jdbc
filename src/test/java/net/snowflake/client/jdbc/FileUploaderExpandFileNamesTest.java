/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.core.OCSPMode;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for SnowflakeFileTransferAgent.expandFileNames */
public class FileUploaderExpandFileNamesTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  @Rule public TemporaryFolder secondFolder = new TemporaryFolder();
  private String localFSFileSep = systemGetProperty("file.separator");

  @Test
  public void testProcessFileNames() throws Exception {
    folder.newFile("TestFileA");
    folder.newFile("TestFileB");

    String folderName = folder.getRoot().getCanonicalPath();
    System.setProperty("user.dir", folderName);
    System.setProperty("user.home", folderName);

    String[] locations = {
      folderName + "/Tes*Fil*A",
      folderName + "/TestFil?B",
      "~/TestFileC",
      "TestFileD",
      folderName + "/TestFileE~"
    };

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations, null);

    assertTrue(files.contains(folderName + "/TestFileA"));
    assertTrue(files.contains(folderName + "/TestFileB"));
    assertTrue(files.contains(folderName + "/TestFileC"));
    assertTrue(files.contains(folderName + "/TestFileD"));
    assertTrue(files.contains(folderName + "/TestFileE~"));
  }

  @Test
  public void testProcessFileNamesException() {
    // inject the Exception
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new Exception());
    String[] locations = {"/Tes*Fil*A", "/TestFil?B", "~/TestFileC", "TestFileD"};

    try {
      SnowflakeFileTransferAgent.expandFileNames(locations, null);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals(200007, err.getErrorCode());
      Assert.assertEquals("22000", err.getSQLState());
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testSnowflakeFileTransferConfig() throws Exception {
    SnowflakeFileTransferMetadataV1 metadata =
        new SnowflakeFileTransferMetadataV1(
            "dummy_presigned_url", null, null, null, null, null, null);

    SnowflakeFileTransferConfig.Builder builder = SnowflakeFileTransferConfig.Builder.newInstance();

    int throwCount = 0;
    int expectedThrowCount = 3;

    // metadata field is needed.
    try {
      builder.build();
    } catch (IllegalArgumentException ex) {
      throwCount++;
    }
    builder.setSnowflakeFileTransferMetadata(metadata);

    // upload stream is needed.
    try {
      builder.build();
    } catch (IllegalArgumentException ex) {
      throwCount++;
    }
    InputStream input =
        new InputStream() {
          @Override
          public int read() throws IOException {
            return 0;
          }
        };
    builder.setUploadStream(input);

    // OCSP mode is needed.
    try {
      builder.build();
    } catch (IllegalArgumentException ex) {
      throwCount++;
    }
    builder.setOcspMode(OCSPMode.FAIL_CLOSED);

    // Set the other optional fields
    builder.setRequireCompress(false);
    builder.setNetworkTimeoutInMilli(12345);
    builder.setPrefix("dummy_prefix");
    Properties props = new Properties();
    builder.setProxyProperties(props);
    builder.setDestFileName("dummy_dest_file_name");

    SnowflakeFileTransferConfig config = builder.build();

    // Assert setting fields are in config
    assertEquals(metadata, config.getSnowflakeFileTransferMetadata());
    assertEquals(input, config.getUploadStream());
    assertEquals(OCSPMode.FAIL_CLOSED, config.getOcspMode());
    assertFalse(config.getRequireCompress());
    assertEquals(12345, config.getNetworkTimeoutInMilli());
    assertEquals(props, config.getProxyProperties());
    assertEquals("dummy_prefix", config.getPrefix());
    assertEquals("dummy_dest_file_name", config.getDestFileName());
    assertEquals(expectedThrowCount, throwCount);
  }

  /**
   * Test fix for if a folder has been deleted before calling FileUtils.listFiles we ignore that
   * directory. Fix available after version 3.16.1.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteDirectoryBeforeListFilesWithWildCard() throws Exception {
    folder.newFile("TestFileA");
    folder.newFile("TestFileB");

    secondFolder.newFile("TestFileC");
    secondFolder.newFile("TestFileD");

    String folderName = folder.getRoot().getCanonicalPath();
    String secondFolderName = secondFolder.getRoot().getCanonicalPath();

    folder.delete();

    String[] locations = {
      folderName + localFSFileSep + "TestFil*A",
      folderName + localFSFileSep + "TestFil*B",
      secondFolderName + localFSFileSep + "TestFil*C",
      secondFolderName + localFSFileSep + "TestFil*D",
    };

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations, null);
    assertFalse(files.contains(folderName + localFSFileSep + "TestFileA"));
    assertFalse(files.contains(folderName + localFSFileSep + "TestFileB"));
    assertTrue(files.contains(secondFolderName + localFSFileSep + "TestFileC"));
    assertTrue(files.contains(secondFolderName + localFSFileSep + "TestFileD"));
    assertEquals(2, files.size());
  }
}
