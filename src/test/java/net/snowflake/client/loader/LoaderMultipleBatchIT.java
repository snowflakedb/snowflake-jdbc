/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.loader;

import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LoaderMultipleBatchIT extends LoaderBase
{
  @Test
  public void testLoaderMultipleBatch() throws Exception
  {
    String refTableName = "LOADER_TEST_TABLE_REF";
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE \"%s\" ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 varchar(255) DEFAULT 'X', "
        + "C3 double, "
        + "C4 timestamp, "
        + "C5 variant)", refTableName));

    try
    {
      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection);
      List<Object[]> dataSet = tdcb.populateReturnData();

      TestDataConfigBuilder tdcbRef = new TestDataConfigBuilder(
          testConnection, putConnection);
      tdcbRef.setDataSet(dataSet)
          .setTableName(refTableName)
          .setCsvFileBucketSize(2)
          .setCsvFileSize(30000).populate();

      ResultSet rsReference = testConnection.createStatement().executeQuery(String.format(
          "SELECT hash_agg(*) FROM \"%s\"", TARGET_TABLE_NAME
      ));
      rsReference.next();
      long hashValueReference = rsReference.getLong(1);
      ResultSet rsTarget = testConnection.createStatement().executeQuery(String.format(
          "SELECT hash_agg(*) FROM \"%s\"", refTableName
      ));
      rsTarget.next();
      long hashValueTarget = rsTarget.getLong(1);
      assertThat("hash values", hashValueTarget, equalTo(hashValueReference));
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", refTableName));
    }
  }


}
