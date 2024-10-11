/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.snowflake.client.core.OCSPMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for SnowflakeFileTransferAgent.expandFileNames */
public class FileUploaderExpandFileNamesTest {
  @TempDir private File folder;
  private String localFSFileSep = systemGetProperty("file.separator");

  @Test
  public void testProcessFileNames() throws Exception {
    new File(folder, "TestFileA").createNewFile();
    new File(folder, "TestFileB").createNewFile();

    String folderName = folder.getCanonicalPath();
    System.setProperty("user.dir", folderName);
    System.setProperty("user.home", folderName);

    String[] locations = {
      folderName + File.separator + "Tes*Fil*A",
      folderName + File.separator + "TestFil?B",
      "~" + File.separator + "TestFileC",
      "TestFileD",
      folderName + File.separator + "TestFileE~"
    };

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations, null);

    Assertions.assertTrue(files.contains(folderName + File.separator + "TestFileA"));
    Assertions.assertTrue(files.contains(folderName + File.separator + "TestFileB"));
    Assertions.assertTrue(files.contains(folderName + File.separator + "TestFileC"));
    Assertions.assertTrue(files.contains(folderName + File.separator + "TestFileD"));
    Assertions.assertTrue(files.contains(folderName + File.separator + "TestFileE~"));
  }

  @Test
  public void testProcessFileNamesException() {
    // inject the Exception
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new Exception());
    String[] locations = {"/Tes*Fil*A", "/TestFil?B", "~/TestFileC", "TestFileD"};

    try {
      SnowflakeFileTransferAgent.expandFileNames(locations, null);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals(200007, err.getErrorCode());
      Assertions.assertEquals("22000", err.getSQLState());
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
    Assertions.assertEquals(metadata, config.getSnowflakeFileTransferMetadata());
    Assertions.assertEquals(input, config.getUploadStream());
    Assertions.assertEquals(OCSPMode.FAIL_CLOSED, config.getOcspMode());
    Assertions.assertFalse(config.getRequireCompress());
    Assertions.assertEquals(12345, config.getNetworkTimeoutInMilli());
    Assertions.assertEquals(props, config.getProxyProperties());
    Assertions.assertEquals("dummy_prefix", config.getPrefix());
    Assertions.assertEquals("dummy_dest_file_name", config.getDestFileName());
    Assertions.assertEquals(expectedThrowCount, throwCount);
  }

  /**
   * We have N jobs expanding files with exclusive pattern, processing them and deleting. Expanding
   * the list should not cause the error when file of another pattern is deleted which may happen
   * when FileUtils.listFiles is used.
   *
   * <p>Fix available after version 3.16.1.
   *
   * @throws Exception
   */
  @Test
  public void testFileListingDoesNotFailOnMissingFilesOfAnotherPattern() throws Exception {
    new File(folder, "TestFiles").mkdirs();
    String folderName = folder.getCanonicalPath();

    int filePatterns = 10;
    int filesPerPattern = 100;
    IntStream.range(0, filesPerPattern * filePatterns)
        .forEach(
            id -> {
              try {
                File file =
                    new File(
                        folderName
                            + localFSFileSep
                            + "foo"
                            + id % filePatterns
                            + "-"
                            + UUID.randomUUID());
                Assertions.assertTrue(file.createNewFile());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    ExecutorService executorService = Executors.newFixedThreadPool(filePatterns / 3);
    List<Future<Set<String>>> futures = new ArrayList<>();
    for (int i = 0; i < filePatterns; ++i) {
      String[] locations = {
        folderName + localFSFileSep + "foo" + i + "*",
      };
      Future<Set<String>> future =
          executorService.submit(
              () -> {
                try {
                  Set<String> strings = SnowflakeFileTransferAgent.expandFileNames(locations, null);
                  strings.forEach(
                      fileName -> {
                        try {
                          File file = new File(fileName);
                          Files.delete(file.toPath());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      });
                  return strings;
                } catch (SnowflakeSQLException e) {
                  throw new RuntimeException(e);
                }
              });
      futures.add(future);
    }
    executorService.shutdown();
    Assertions.assertTrue(executorService.awaitTermination(60, TimeUnit.SECONDS));
    Assertions.assertEquals(filePatterns, futures.size());
    for (Future<Set<String>> future : futures) {
      Assertions.assertTrue(future.isDone());
      Assertions.assertEquals(filesPerPattern, future.get().size());
    }
  }

  @Test
  public void testFileListingDoesNotFailOnNotExistingDirectory() throws Exception {
    new File(folder, "TestFiles").mkdirs();
    String folderName = folder.getCanonicalPath();
    String[] locations = {
      folderName + localFSFileSep + "foo*",
    };
    folder.delete();

    Set<String> files = SnowflakeFileTransferAgent.expandFileNames(locations, null);

    Assertions.assertTrue(files.isEmpty());
  }
}
