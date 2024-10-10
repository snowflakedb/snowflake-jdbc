/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class SnowflakeClobTest extends BaseJDBCTest {

  @Test
  public void testReadCharacterStream() throws SQLException, IOException {
    SnowflakeClob clob = new SnowflakeClob("hello world");
    char[] chars = new char[100];
    Reader reader = clob.getCharacterStream(1, clob.length());
    int charRead;
    charRead = reader.read(chars, 0, chars.length);
    assertEquals(charRead, 11);
    assertEquals("hello world", clob.toString());
  }

  @Test
  public void testReadWriteAsciiStream() throws SQLException, IOException {
    SnowflakeClob clob = new SnowflakeClob("hello world");
    clob.setAsciiStream(1);
    char[] chars = new char[100];
    InputStream input = clob.getAsciiStream();
    int charRead;
    Reader in = new InputStreamReader(input, StandardCharsets.UTF_8);
    charRead = in.read(chars, 0, chars.length);
    assertEquals(charRead, 11);
  }

  @Test
  public void testFreeBuffer() throws SQLException, IOException {
    SnowflakeClob clob = new SnowflakeClob("hello world");
    clob.setCharacterStream(1).close();
    assertEquals(11, clob.length());
    clob.free();
    assertEquals(0, clob.length());
  }

  @Test
  public void testGetSubString() throws SQLException {
    SnowflakeClob clob = new SnowflakeClob();
    clob.setString(1, "hello world", 0, 11);
    assertEquals("world", clob.getSubString(6, 5));
    assertEquals(0, clob.position("hello", 1));
    assertEquals(0, clob.position(new SnowflakeClob("hello world"), 1));
  }

  @Test
  public void testInvalidPositionExceptions() {
    SnowflakeClob clob = new SnowflakeClob();

    try {
      clob.setString(0, "this should throw an exception");
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof SQLException);
    }

    try {
      clob.setString(0, "this should throw an exception", 0, 5);
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof SQLException);
    }

    try {
      clob.getSubString(0, 1);
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof SQLException);
    }

    try {
      clob.position("this should throw an exception", 0);
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof SQLException);
    }

    try {
      clob.position(new SnowflakeClob("this should throw an exception"), 0);
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof SQLException);
    }
  }
}
