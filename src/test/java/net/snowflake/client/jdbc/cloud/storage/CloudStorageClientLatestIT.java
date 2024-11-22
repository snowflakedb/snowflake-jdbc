package net.snowflake.client.jdbc.cloud.storage;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag(TestTags.OTHERS)
public class CloudStorageClientLatestIT extends BaseJDBCTest {

  /**
   * Test for SNOW-565154 - it was waiting for ~5 minutes so the test is waiting much shorter time
   */
  @Test
  @Timeout(30)
  public void testDownloadStreamShouldFailFastOnNotExistingFile() throws Throwable {
    String stageName =
        "testDownloadStream_stage_" + UUID.randomUUID().toString().replaceAll("-", "_");
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE TEMP STAGE " + stageName);

        try (InputStream out =
            connection
                .unwrap(SnowflakeConnection.class)
                .downloadStream("@" + stageName, "/fileNotExist.gz", true)) {
          fail("file should not exist");
        } catch (Throwable e) {
          assertThat(e, instanceOf(SQLException.class));
        }
      } finally {
        statement.execute("DROP STAGE IF EXISTS " + stageName);
      }
    }
  }
}
