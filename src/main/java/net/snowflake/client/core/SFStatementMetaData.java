/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/**
 * Created by jhuang on 1/21/16.
 */

/**
 * Statement metadata which includes the result metadata and bind information.
 */
public class SFStatementMetaData
{
  // result metadata
  private SFResultSetMetaData resultSetMetaData;

  // number of binds
  private int numberOfBinds;

  public SFResultSetMetaData getResultSetMetaData()
  {
    return resultSetMetaData;
  }

  public void setResultSetMetaData(SFResultSetMetaData resultSetMetaData)
  {
    this.resultSetMetaData = resultSetMetaData;
  }

  public int getNumberOfBinds()
  {
    return numberOfBinds;
  }

  public void setNumberOfBinds(int numberOfBinds)
  {
    this.numberOfBinds = numberOfBinds;
  }

  public SFStatementMetaData(SFResultSetMetaData resultSetMetaData,
                             int numberOfBinds)
  {
    this.resultSetMetaData = resultSetMetaData;
    this.numberOfBinds = numberOfBinds;
  }
}
