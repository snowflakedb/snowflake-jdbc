package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import net.snowflake.client.category.TestCategoryLoader;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Loader API tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to LoaderIT so that both the latest and oldest
 * supported driver run the tests.
 */
@Category(TestCategoryLoader.class)
public class LoaderLatestIT extends LoaderBase {
  @Test
  public void testLoaderUpsert() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setColumns(Arrays.asList("ID", "C1", "C2", "C3", "C4", "C5"))
        .setKeys(Collections.singletonList("ID"));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    loader.start();

    Date d = new Date();

    Object[] ups = new Object[] {10001, "inserted\\,", "something", 0x4.11_33p2, d, "{}"};
    loader.submitRow(ups);
    ups = new Object[] {39, "modified", "something", 40.1, d, "{}"};
    loader.submitRow(ups);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format(
                    "SELECT C1, C4, C3" + " FROM \"%s\" WHERE ID=10001", TARGET_TABLE_NAME));

    rs.next();
    assertThat("C1 is not correct", rs.getString("C1"), equalTo("inserted\\,"));

    long l = rs.getTimestamp("C4").getTime();
    assertThat("C4 is not correct", l, equalTo(d.getTime()));
    assertThat(
        "C3 is not correct", Double.toHexString((rs.getDouble("C3"))), equalTo("0x1.044ccp4"));

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT C1 AS N" + " FROM \"%s\" WHERE ID=39", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testLoaderUpsertWithErrorAndRollback() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    PreparedStatement pstmt =
        testConnection.prepareStatement(
            String.format(
                "INSERT INTO \"%s\"(ID,C1,C2,C3,C4,C5)"
                    + " SELECT column1, column2, column3, column4,"
                    + " column5, parse_json(column6)"
                    + " FROM VALUES(?,?,?,?,?,?)",
                TARGET_TABLE_NAME));
    pstmt.setInt(1, 10001);
    pstmt.setString(2, "inserted\\,");
    pstmt.setString(3, "something");
    pstmt.setDouble(4, 0x4.11_33p2);
    pstmt.setDate(5, new java.sql.Date(new Date().getTime()));
    pstmt.setObject(6, "{}");
    pstmt.execute();
    testConnection.commit();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setStartTransaction(true)
        .setPreserveStageFile(true)
        .setColumns(Arrays.asList("ID", "C1", "C2", "C3", "C4", "C5"))
        .setKeys(Collections.singletonList("ID"));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    listener.throwOnError = true; // should trigger rollback
    loader.start();
    try {

      Object[] noerr = new Object[] {"10001", "inserted", "something", "42", new Date(), "{}"};
      loader.submitRow(noerr);

      Object[] err = new Object[] {"10002-", "inserted", "something", "42-", new Date(), "{}"};
      loader.submitRow(err);

      loader.finish();

      fail("Test must raise Loader.DataError exception");
    } catch (Loader.DataError e) {
      // we are good
      assertThat(
          "error message",
          e.getMessage(),
          allOf(containsString("10002-"), containsString("not recognized")));
    }

    assertThat("processed", listener.processed.get(), equalTo(0));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(0));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));

    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(String.format("SELECT COUNT(*) AS N FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("N", rs.getInt("N"), equalTo(10001));

    rs =
        testConnection
            .createStatement()
            .executeQuery(String.format("SELECT C3 FROM \"%s\" WHERE id=10001", TARGET_TABLE_NAME));
    rs.next();
    assertThat(
        "C3. No commit should happen",
        Double.toHexString((rs.getDouble("C3"))),
        equalTo("0x1.044ccp4"));
  }
}
