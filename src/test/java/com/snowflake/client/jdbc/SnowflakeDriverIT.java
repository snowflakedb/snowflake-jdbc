package com.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;

import ch.qos.logback.core.recovery.ResilientFileOutputStream;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.loader.BufferStage;
import org.junit.Test;

public class SnowflakeDriverIT extends AbstractDriverIT {

  @Test
  public void testConnection() throws SQLException {
      Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, true);
      System.out.println("Create JDBC statement");
      Statement stmt = con.createStatement();
      System.out.println("Done creating JDBC statement\n");
      //ResultSet rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 98333");
      //int count = 0;


      int result1 = 0, result2 = 0;
      //ResultSet rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 4562");
      //ResultSet rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 4560");
      //ResultSet rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 777");
      ResultSet rs = stmt.executeQuery("put file:///tmp/fips_s3_test.csv @~");
      //rs = stmt.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from user_share_db.harsh_test_schema.harsh_test_table limit 10;");
      while(rs.next())
          result1 = rs.getInt("YCSB_KEY");
      //rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 2745");
      rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where ycsb_key = 274");
      while(rs.next())
          result2 = rs.getInt("YCSB_KEY");
      System.out.println("Result1 : "+result1+" Result2: "+result2);
      rs.close();
      stmt.close();
      con.close();
  }
}
