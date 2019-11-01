package net.snowflake.client.jdbc;

public class PreparedMultiStmtArrowIT extends PreparedMultiStmtIT
{
  public PreparedMultiStmtArrowIT()
  {
    super();
    queryResultFormat = "arrow";
  }
}
