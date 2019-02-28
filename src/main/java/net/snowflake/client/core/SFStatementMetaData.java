/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/**
 * Created by jhuang on 1/21/16.
 */

import java.util.Collections;

/**
 * Statement metadata which includes the result metadata and bind information.
 */
public class SFStatementMetaData
{
  // result metadata
  private SFResultSetMetaData resultSetMetaData;

  // number of binds
  private int numberOfBinds;

  private final SFStatementType statementType;

  private final boolean arrayBindSupported;

  SFStatementMetaData(SFResultSetMetaData resultSetMetaData,
                      SFStatementType statementType,
                      int numberOfBinds,
                      boolean arrayBindSupported)
  {
    this.resultSetMetaData = resultSetMetaData;
    this.statementType = statementType;
    this.numberOfBinds = numberOfBinds;
    this.arrayBindSupported = arrayBindSupported;
  }

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

  /**
   * According to StatementType, to decide whether array binds supported or not
   *
   * Currently, only INSERT supports array bind
   * @return true if array binds is supported.
   */
  public boolean isArrayBindSupported()
  {
    return this.arrayBindSupported;
  }

  public SFStatementType getStatementType()
  {
    return this.statementType;
  }

  public static SFStatementMetaData emptyMetaData()
  {
    return new SFStatementMetaData(
        new SFResultSetMetaData(0,
                                Collections.<String>emptyList(),
                                Collections.<String>emptyList(),
                                Collections.<Integer>emptyList(),
                                null),
        SFStatementType.UNKNOWN,
        0,
        false);
  }
}
