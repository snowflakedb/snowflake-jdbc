/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
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

  @Test
  public void testProcessFileNames() throws Exception {
    folder.newFile("TestFileA");
    folder.newFile("TestFileB");

    String folderName = folder.getRoot().getCanonicalPath();
    System.setProperty("user.dir", folderName);
    System.setProperty("user.home", folderName);

    String[] locations = {
            folderName + File.separator + "Tes*Fil*A",
            folderName + File.separator + "TestFil?B",
            "~" +  File.separator + "TestFileC",
            "TestFileD",
            folderName + File.separator + "TestFileE~"
    };

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations, null);

    assertTrue(files.contains(folderName + File.separator + "TestFileA"));
    assertTrue(files.contains(folderName + File.separator + "TestFileB"));
    assertTrue(files.contains(folderName + File.separator + "TestFileC"));
    assertTrue(files.contains(folderName + File.separator + "TestFileD"));
    assertTrue(files.contains(folderName + File.separator + "TestFileE~"));
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
}
