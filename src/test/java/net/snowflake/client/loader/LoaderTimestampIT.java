package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.LOADER)
public class LoaderTimestampIT extends LoaderBase {
  @Test
  public void testLoadTimestamp() throws Exception {
    final String targetTableName = "LOADER_TEST_TIMESTAMP";

    // create table including TIMESTAMP_NTZ
    try (Statement statement = testConnection.createStatement()) {
      statement.execute(
          String.format(
              "CREATE OR REPLACE TABLE %s ("
                  + "ID int, "
                  + "C1 varchar(255), "
                  + "C2 timestamp_ntz)",
              targetTableName));

      // Binding java.util.Date, Timestamp and java.sql.Date with TIMESTAMP
      // datatype. No java.sql.Time binding is supported for TIMESTAMP.
      // For java.sql.Time, the target data type must be TIME.
      Object[] testData =
          new Object[] {
            new Date(),
            java.sql.Timestamp.valueOf("0001-01-01 08:00:00"),
            java.sql.Date.valueOf("2001-01-02")
          };

      for (int i = 0; i < 2; ++i) {
        boolean useLocalTimezone = false;
        TimeZone originalTimeZone;
        TimeZone targetTimeZone;

        if (i == 0) {
          useLocalTimezone = true;
          originalTimeZone = TimeZone.getDefault();
          targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
        } else {
          originalTimeZone = TimeZone.getTimeZone("UTC");
          targetTimeZone = TimeZone.getTimeZone("UTC");
        }

        // input timestamp associated with the target timezone, America/Los_Angeles
        for (Object testTs : testData) {
          _testLoadTimestamp(
              targetTableName, originalTimeZone, targetTimeZone, testTs, useLocalTimezone, false);
        }
      }
    }
  }

  private void _testLoadTimestamp(
      String targetTableName,
      TimeZone originalTimeZone,
      TimeZone targetTimeZone,
      Object testTs,
      boolean useLocalTimeZone,
      boolean mapTimeToTimestamp)
      throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);

    tdcb.setStartTransaction(true)
        .setTruncateTable(true)
        .setTableName(targetTableName)
        .setUseLocalTimezone(useLocalTimeZone)
        .setMapTimeToTimestamp(mapTimeToTimestamp)
        .setColumns(Arrays.asList("ID", "C1", "C2"));
    StreamLoader loader = tdcb.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();

    TimeZone.setDefault(targetTimeZone); // change default timezone before start

    loader.start();

    for (int i = 0; i < 5; ++i) {
      Object[] row = new Object[] {i, "foo_" + i, testTs};
      loader.submitRow(row);
    }
    loader.finish();
    TimeZone.setDefault(originalTimeZone);

    assertThat("Loader detected errors", listener.getErrorCount(), equalTo(0));

    try (ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(String.format("SELECT * FROM \"%s\"", targetTableName))) {

      assertTrue(rs.next());
      Timestamp ts = rs.getTimestamp("C2");

      // format the input TS with the target timezone
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      sdf.setTimeZone(targetTimeZone);
      String currentTsStr = sdf.format(testTs);

      // format the retrieved TS with the original timezone
      sdf.setTimeZone(originalTimeZone);
      String retrievedTsStr = sdf.format(new Date(ts.getTime()));

      // They must be identical.
      assertThat(
          "Input and retrieved timestamp are different", retrievedTsStr, equalTo(currentTsStr));
    }
  }

  @Test
  public void testLoadTimestampV1() throws Exception {
    final String targetTableName = "LOADER_TEST_TIMESTAMP_V1";

    // create table including TIMESTAMP_NTZ
    try (Statement statement = testConnection.createStatement()) {
      statement.execute(
          String.format(
              "CREATE OR REPLACE TABLE %s ("
                  + "ID int, "
                  + "C1 varchar(255), "
                  + "C2 timestamp_ntz)",
              targetTableName));

      // Binding java.sql.Time with TIMESTAMP is supported only if
      // mapTimeToTimestamp flag is enabled. This is required to keep the
      // old behavior of Informatica V1 connector.
      Object[] testData =
          new Object[] {
            // full timestamp in Time object. Interestingly all values are
            // preserved.
            new java.sql.Time(1502931205000L),
            java.sql.Time.valueOf("12:34:56") // a basic test case
          };

      for (int i = 0; i < 2; ++i) {
        boolean useLocalTimezone;
        TimeZone originalTimeZone;
        TimeZone targetTimeZone;

        if (i == 0) {
          useLocalTimezone = true;
          originalTimeZone = TimeZone.getDefault();
          targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
        } else {
          useLocalTimezone = false;
          originalTimeZone = TimeZone.getTimeZone("UTC");
          targetTimeZone = TimeZone.getTimeZone("UTC");
        }

        // input timestamp associated with the target timezone, America/Los_Angeles
        for (Object testTs : testData) {
          _testLoadTimestamp(
              targetTableName, originalTimeZone, targetTimeZone, testTs, useLocalTimezone, true);
        }
      }
    }
  }
}
