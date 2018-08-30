/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;

import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * @author hyu
 */
public class BaseJDBCTest extends AbstractDriverIT
{
  private static Logger logger = Logger.getLogger(BaseJDBCTest.class.getName());

  int getSizeOfResultSet(ResultSet rs) throws SQLException
  {
    int count = 0;
    while (rs.next())
    {
      count++;
    }
    return count;
  }

  String getSFProjectRootString() throws Exception
  {
    URL location =
        BaseJDBCTest.class.getProtectionDomain().getCodeSource().getLocation();
    String testDir = URLDecoder.decode(location.getPath(), "UTF-8");
    System.out.println(testDir);
    return testDir.substring(0, testDir.indexOf("Client"));
  }

  ArrayList<String> getInfoViaSQLCmd(String sqlCmd) throws SQLException
  {
    Connection con = getConnection();
    Statement st = con.createStatement();
    ArrayList<String> result = new ArrayList<>();
    ResultSet rs = st.executeQuery(sqlCmd);
    while (rs.next())
    {
      result.add(rs.getString(1));
    }
    return result;
  }
}