/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.snowflake.client.core;

/**
 * Used to check if the statementType belongs to DDL or DML
 * The enum of each statement type is defined in 
 * com.snowflake.core.Statement.java
 * @author hyu
 */
public enum SFStatementType
{
  UNKNOWN(0x0000),

  // Server does not have put/get command type
  PUT(0x0001),
  GET(0x0002),

  SELECT(0x1000),

  // all dml are listed
  DML(0x3000),
  INSERT(0x3100),
  UPDATE(0x3200),
  DELETE(0x3300),
  MERGE(0x3400),
  MULTI_INSERT(0x3500),
  COPY(0x3600),

  // only general ddl is listed
  DDL(0x6000);

  private final long statementTypeId;

  private static final long LEVEL_3_RANGE = 0x1000;

  SFStatementType(long id)
  {
    this.statementTypeId = id;
  }

  public static SFStatementType lookUpTypeById(long id)
  {
    for (SFStatementType type : SFStatementType.values())
    {
      if (type.getStatementTypeId() == id)
      {
        return type;
      }
      else if (id >= DDL.getStatementTypeId() && id < DDL.getStatementTypeId()
          + LEVEL_3_RANGE )
      {
        return DDL;
      }
    }

    return UNKNOWN;
  }

  public long getStatementTypeId()
  {
    return statementTypeId;
  }

  public boolean isDDL()
  {
    return this == DDL;
  }

  public boolean isDML()
  {
    return statementTypeId >= DML.getStatementTypeId() &&
           statementTypeId < DML.getStatementTypeId() + LEVEL_3_RANGE;
  }
}
