/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/**
 * Created by jhuang on 1/21/16.
 */

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.common.core.SqlState;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  private List<MetaDataOfBinds> metaDataOfBinds;

  SFStatementMetaData(SFResultSetMetaData resultSetMetaData,
                      SFStatementType statementType,
                      int numberOfBinds,
                      boolean arrayBindSupported,
                      List<MetaDataOfBinds> metaDataOfBinds)
  {
    this.resultSetMetaData = resultSetMetaData;
    this.statementType = statementType;
    this.numberOfBinds = numberOfBinds;
    this.arrayBindSupported = arrayBindSupported;
    this.metaDataOfBinds = metaDataOfBinds;
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

  public MetaDataOfBinds getMetaDataForBindParam (int param) throws SQLException
  {
    if (param < 1 || param > numberOfBinds)
    {
      throw new SnowflakeSQLException(SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
                                      ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE.getMessageCode(), param,
                                      numberOfBinds);
    }
    if (numberOfBinds != metaDataOfBinds.size() || metaDataOfBinds.size() == 0)
    {
      throw new SnowflakeSQLException(SqlState.NO_DATA, ErrorCode.NO_VALID_DATA.getMessageCode());
    }
    return metaDataOfBinds.get(param-1);
  }

  public void setNumberOfBinds(int numberOfBinds)
  {
    this.numberOfBinds = numberOfBinds;
  }

  /**
   * According to StatementType, to decide whether array binds supported or not
   * <p>
   * Currently, only INSERT supports array bind
   *
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
        false,
        new ArrayList<>());
  }
}
