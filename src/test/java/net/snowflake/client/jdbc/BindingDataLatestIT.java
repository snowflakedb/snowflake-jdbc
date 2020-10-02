/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Binding Data integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the version of oldest supported driver
 * to examine if the tests still are not applicable. If it is applicable, move tests to
 * BindingDataIT so that both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class BindingDataLatestIT extends AbstractDriverIT {
  @Test
  public void testBindTimestampTZ() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table testBindTimestampTZ(" + "cola int, colb timestamp_tz)");
    statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_TZ");

    long millSeconds = System.currentTimeMillis();
    Timestamp ts = new Timestamp(millSeconds);
    PreparedStatement prepStatement =
        connection.prepareStatement("insert into testBindTimestampTZ values (?, ?)");
    prepStatement.setInt(1, 123);
    prepStatement.setTimestamp(2, ts, Calendar.getInstance(TimeZone.getTimeZone("EST")));
    prepStatement.execute();

    ResultSet resultSet = statement.executeQuery("select cola, colb from testBindTimestampTz");
    resultSet.next();
    assertThat("integer", resultSet.getInt(1), equalTo(123));
    assertThat("timestamp_tz", resultSet.getTimestamp(2), equalTo(ts));
  }
}
