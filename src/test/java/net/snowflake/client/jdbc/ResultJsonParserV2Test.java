package net.snowflake.client.jdbc;

import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 * <p>
 * This is the unit tests for ResultJsonParserV2
 */
public class ResultJsonParserV2Test
{
  @Test
  public void simpleTest() throws SnowflakeSQLException
  {
    String simple = "[\"1\", \"1.01\"]," +
                    "[null, null]," +
                    "[\"2\", \"0.13\"]," +
                    "[\"\", \"\"]," +
                    "[\"\\\"escape\\\"\", \"\\\"escape\\\"\"]," +
                    "[\"\\u2605\", \"\\u263A\\u263A\"]," +
                    "[\"\\ud841\\udf0e\", \"\\ud841\\udf31\\ud841\\udf79\"]," +
                    "[\"{\\\"date\\\" : \\\"2017-04-28\\\",\\\"dealership\\\" : \\\"Tindel Toyota\\\"}\", \"[1,2,3,4,5]\"]";
    byte[] data = simple.getBytes(StandardCharsets.UTF_8);
    SnowflakeResultChunk chunk = new SnowflakeResultChunk("", 8, 2, data.length, true);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk);
    jp.continueParsing(ByteBuffer.wrap(data));
    jp.endParsing();
    assertEquals("1", chunk.getCell(0, 0).toString());
    assertEquals("1.01", chunk.getCell(0, 1).toString());
    assertNull(chunk.getCell(1, 0));
    assertNull(chunk.getCell(1, 1));
    assertEquals("2", chunk.getCell(2, 0).toString());
    assertEquals("0.13", chunk.getCell(2, 1).toString());
    assertEquals("", chunk.getCell(3, 0).toString());
    assertEquals("", chunk.getCell(3, 1).toString());
    assertEquals("\"escape\"", chunk.getCell(4, 0).toString());
    assertEquals("\"escape\"", chunk.getCell(4, 1).toString());
    assertEquals("★", chunk.getCell(5, 0).toString());
    assertEquals("☺☺", chunk.getCell(5, 1).toString());
    assertEquals("𠜎", chunk.getCell(6, 0).toString());
    assertEquals("𠜱𠝹", chunk.getCell(6, 1).toString());
    assertEquals("{\"date\" : \"2017-04-28\",\"dealership\" : \"Tindel Toyota\"}", chunk.getCell(7, 0).toString());
    assertEquals("[1,2,3,4,5]", chunk.getCell(7, 1).toString());
  }

  @Test
  public void simpleStreamingTest() throws SnowflakeSQLException
  {
    String simple = "[\"1\", \"1.01\"]," +
                    "[null, null]," +
                    "[\"2\", \"0.13\"]," +
                    "[\"\", \"\"]," +
                    "[\"\\\"escape\\\"\", \"\\\"escape\\\"\"]," +
                    "[\"☺☺\", \"☺☺☺\"], " +
                    "[\"\\ud841\\udf0e\", \"\\ud841\\udf31\\ud841\\udf79\"]," +
                    "[\"{\\\"date\\\" : \\\"2017-04-28\\\",\\\"dealership\\\" : \\\"Tindel Toyota\\\"}\", \"[1,2,3,4,5]\"]";
    byte[] data = simple.getBytes(StandardCharsets.UTF_8);
    SnowflakeResultChunk chunk = new SnowflakeResultChunk("", 8, 2, data.length, true);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk);
    int len = 2;
    for (int i = 0; i < data.length; i += len)
    {
      if (i + len < data.length)
      {
        jp.continueParsing(ByteBuffer.wrap(data, i, len));
      }
      else
      {
        jp.continueParsing(ByteBuffer.wrap(data, i, data.length - i));
      }
    }
    jp.endParsing();
    assertEquals("1", chunk.getCell(0, 0).toString());
    assertEquals("1.01", chunk.getCell(0, 1).toString());
    assertNull(chunk.getCell(1, 0));
    assertNull(chunk.getCell(1, 1));
    assertEquals("2", chunk.getCell(2, 0).toString());
    assertEquals("0.13", chunk.getCell(2, 1).toString());
    assertEquals("", chunk.getCell(3, 0).toString());
    assertEquals("", chunk.getCell(3, 1).toString());
    assertEquals("\"escape\"", chunk.getCell(4, 0).toString());
    assertEquals("\"escape\"", chunk.getCell(4, 1).toString());
    assertEquals("☺☺", chunk.getCell(5, 0).toString());
    assertEquals("☺☺☺", chunk.getCell(5, 1).toString());
    assertEquals("𠜎", chunk.getCell(6, 0).toString());
    assertEquals("𠜱𠝹", chunk.getCell(6, 1).toString());
    assertEquals("{\"date\" : \"2017-04-28\",\"dealership\" : \"Tindel Toyota\"}", chunk.getCell(7, 0).toString());
    assertEquals("[1,2,3,4,5]", chunk.getCell(7, 1).toString());

  }

  /**
   * Test the largest column size 16 MB
   *
   * @throws SnowflakeSQLException
   */
  @Test
  public void LargestColumnTest() throws SnowflakeSQLException
  {
    StringBuilder sb = new StringBuilder();
    StringBuilder a = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++)
    {
      a.append("a");
    }
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++)
    {
      b.append("b");
    }
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++)
    {
      c.append("c");
    }
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024 - 5; i += 6)
    {
      s.append("\\u263A");
    }
    sb.append("[\"").append(a).append("\",\"").append(b)
        .append("\"],[\"").append(c).append("\",\"").append(s).append("\"]");


    byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
    SnowflakeResultChunk chunk = new SnowflakeResultChunk("", 2, 2, data.length, true);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk);
    jp.continueParsing(ByteBuffer.wrap(data));
    jp.endParsing();
    assertEquals(a.toString(), chunk.getCell(0, 0).toString());
    assertEquals(b.toString(), chunk.getCell(0, 1).toString());
    assertEquals(c.toString(), chunk.getCell(1, 0).toString());
    assertEquals(StringEscapeUtils.unescapeJava(s.toString()), chunk.getCell(1, 1).toString());
  }
}
