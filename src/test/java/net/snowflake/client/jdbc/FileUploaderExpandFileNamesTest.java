package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    String originalUserDir = System.getProperty("user.dir");
    String originalUserHome = System.getProperty("user.home");
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

    assertTrue(files.contains(folderName + File.separator + "TestFileA"));
    assertTrue(files.contains(folderName + File.separator + "TestFileB"));
    assertTrue(files.contains(folderName + File.separator + "TestFileC"));
    assertTrue(files.contains(folderName + File.separator + "TestFileD"));
    assertTrue(files.contains(folderName + File.separator + "TestFileE~"));

    if (originalUserHome != null) {
      System.setProperty("user.home", originalUserHome);
    } else {
      System.clearProperty("user.home");
    }
    if (originalUserDir != null) {
      System.setProperty("user.dir", originalUserDir);
    } else {
      System.clearProperty("user.dir");
    }
  }

  @Test
  public void testProcessFileNamesException() {
    // inject the Exception
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new Exception());
    String[] locations = {"/Tes*Fil*A", "/TestFil?B", "~/TestFileC", "TestFileD"};

    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.expandFileNames(locations, null));
    assertEquals(200007, err.getErrorCode());
    assertEquals("22000", err.getSQLState());
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
                assertTrue(file.createNewFile());
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
    assertTrue(executorService.awaitTermination(60, TimeUnit.SECONDS));
    assertEquals(filePatterns, futures.size());
    for (Future<Set<String>> future : futures) {
      assertTrue(future.isDone());
      assertEquals(filesPerPattern, future.get().size());
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

    assertTrue(files.isEmpty());
  }
}
