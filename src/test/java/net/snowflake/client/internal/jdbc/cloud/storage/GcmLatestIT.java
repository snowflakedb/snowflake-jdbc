package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.randomAlphaNumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.jdbc.BaseJDBCTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
class GcmLatestIT extends BaseJDBCTest {
  @Test
  void testPutGet() throws Exception {
    try (Connection conn = getConnection();
        Statement statement = conn.createStatement()) {
      String stageName = "test_gcm_put_get_" + randomAlphaNumeric(5);
      try {
        //        statement.execute("ALTER SESSION SET FORCE_STAGE_CLIENT_ENCRYPTION_CIPHERS =
        // 'AES_GCM'");
        statement.execute("CREATE OR REPLACE STAGE " + stageName);
        statement.execute(
            "PUT file://"
                + getFullPathFileInResource(TEST_DATA_FILE)
                + " @"
                + stageName
                + " AUTO_COMPRESS=false");
        Path tempDirectory = Files.createTempDirectory("gcm_table_unload");
        statement.execute("GET @" + stageName + " file://" + tempDirectory.toString());
        File[] files = tempDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
        try (InputStream downloadedFileStream =
            Files.newInputStream(Paths.get(getFullPathFileInResource(TEST_DATA_FILE)))) {
          assertEquals(
              IOUtils.readLines(downloadedFileStream, StandardCharsets.UTF_8),
              Files.readAllLines(files[0].toPath()));
        }
      } finally {
        statement.execute("DROP STAGE IF EXISTS " + stageName);
      }
    }
  }

  @Test
  void testTableUnload() throws Exception {
    try (Connection conn = getConnection();
        Statement statement = conn.createStatement()) {
      try {
        //        statement.execute("ALTER SESSION SET FORCE_STAGE_CLIENT_ENCRYPTION_CIPHERS =
        // 'AES_GCM'");
        statement.execute("CREATE TABLE test_gcm_table_unload (id INT)");
        statement.execute("INSERT INTO test_gcm_table_unload VALUES (1), (2)");
        statement.execute(
            "COPY INTO @%test_gcm_table_unload/output/ FROM test_gcm_table_unload FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = NONE)");
        Path tempDirectory = Files.createTempDirectory("gcm_table_unload");
        statement.execute("GET @%test_gcm_table_unload file://" + tempDirectory.toString());
        File[] files = tempDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
        assertEquals(Arrays.asList("1", "2"), Files.readAllLines(files[0].toPath()));
      } finally {
        statement.execute("DROP TABLE IF EXISTS test_gcm_table_unload");
      }
    }
  }
}
