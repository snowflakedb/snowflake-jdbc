/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BaseJDBCTest extends AbstractDriverIT
{
  int getSizeOfResultSet(ResultSet rs) throws SQLException
  {
    int count = 0;
    while (rs.next())
    {
      count++;
    }
    return count;
  }

  List<String> getInfoViaSQLCmd(String sqlCmd) throws SQLException
  {
    Connection con = getConnection();
    Statement st = con.createStatement();
    List<String> result = new ArrayList<>();
    ResultSet rs = st.executeQuery(sqlCmd);
    while (rs.next())
    {
      result.add(rs.getString(1));
    }
    return result;
  }

  boolean isEqualTwoCollecionts(Collection<String> a, Collection<String> b)
  {
    if (a.size() != b.size())
    {
      return false;
    }
    for (String elem : a)
    {
      if (!b.contains(elem))
      {
        return false;
      }
    }
    return true;
  }
}