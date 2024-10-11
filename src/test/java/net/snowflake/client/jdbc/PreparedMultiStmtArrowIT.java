package net.snowflake.client.jdbc;

// @Category(TestCategoryArrow.class)
public class PreparedMultiStmtArrowIT extends PreparedMultiStmtIT {
  public PreparedMultiStmtArrowIT() {
    super();
    queryResultFormat = "arrow";
  }
}
