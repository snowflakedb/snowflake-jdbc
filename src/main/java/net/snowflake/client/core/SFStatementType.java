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
public class SFStatementType
{
  public static boolean isSelectStatement(long id)
  {
    if (id == 0x1000)
      return true;
    else 
      return false;
  }

  public static boolean isDML(long id)
  {
    if (id >= 0x3000 && id < 0x4000 )
      return true;
    else
      return false;
  }

  public static boolean isDDL(long id)
  {
    if (id >= 0x6000)
      return true;
    else 
      return false;
  }
}
