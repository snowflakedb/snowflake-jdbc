package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.LOADER)
public class LoaderMultipleBatchIT extends LoaderBase {
  @Test
  public void testLoaderMultipleBatch() throws Exception {
    String refTableName = "LOADER_TEST_TABLE_REF";
    try (Statement statement = testConnection.createStatement()) {
      statement.execute(
          String.format(
              "CREATE OR REPLACE TABLE \"%s\" ("
                  + "ID int, "
                  + "C1 varchar(255), "
                  + "C2 varchar(255) DEFAULT 'X', "
                  + "C3 double, "
                  + "C4 timestamp, "
                  + "C5 variant)",
              refTableName));

      try {
        TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
        List<Object[]> dataSet = tdcb.populateReturnData();

        TestDataConfigBuilder tdcbRef = new TestDataConfigBuilder(testConnection, putConnection);
        tdcbRef
            .setDataSet(dataSet)
            .setTableName(refTableName)
            .setCsvFileBucketSize(2)
            .setCsvFileSize(30000)
            .populate();

        try (ResultSet rsReference =
            statement.executeQuery(
                String.format("SELECT hash_agg(*) FROM \"%s\"", TARGET_TABLE_NAME))) {
          assertTrue(rsReference.next());
          long hashValueReference = rsReference.getLong(1);
          try (ResultSet rsTarget =
              statement.executeQuery(
                  String.format("SELECT hash_agg(*) FROM \"%s\"", refTableName))) {
            assertTrue(rsTarget.next());
            long hashValueTarget = rsTarget.getLong(1);
            assertThat("hash values", hashValueTarget, equalTo(hashValueReference));
          }
        }
      } finally {
        statement.execute(String.format("DROP TABLE IF EXISTS %s", refTableName));
      }
    }
  }
}
