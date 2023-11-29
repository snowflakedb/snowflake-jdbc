package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class QueryStatusTest {
  @Test
  public void testThreadSafetyByConstructor() {
    QueryStatus status1 =
        new QueryStatus(QueryStatus.RUNNING.getValue(), QueryStatus.RUNNING.getDescription());
    QueryStatus status2 =
        new QueryStatus(QueryStatus.RUNNING.getValue(), QueryStatus.RUNNING.getDescription());
    status1.setErrorCode(1);
    status2.setErrorCode(2);

    assertEquals(status1.getErrorCode(), 1);
    assertEquals(status2.getErrorCode(), 2);
  }

  @Test
  public void testThreadSafetyByDescription() {
    QueryStatus status1 = QueryStatus.getStatusFromString(QueryStatus.RUNNING.getDescription());
    QueryStatus status2 = QueryStatus.getStatusFromString(QueryStatus.RUNNING.getDescription());
    status1.setErrorCode(1);
    status2.setErrorCode(2);

    assertEquals(status1.getErrorCode(), 1);
    assertEquals(status2.getErrorCode(), 2);
  }

  @Test
  public void testEquality() {
    assertEquals(
        QueryStatus.RUNNING,
        new QueryStatus(QueryStatus.RUNNING.getValue(), QueryStatus.RUNNING.getDescription()));
  }
}
