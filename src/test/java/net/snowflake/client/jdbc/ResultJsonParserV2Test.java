package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.core.SFSession;
import org.apache.commons.text.StringEscapeUtils;
import org.junit.jupiter.api.Test;

/** This is the unit tests for ResultJsonParserV2 */
public class ResultJsonParserV2Test {
  @Test
  public void simpleTest() throws SnowflakeSQLException {
    SFSession session = null;
    String simple =
        "[\"1\", \"1.01\"],"
            + "[null, null],"
            + "[\"2\", \"0.13\"],"
            + "[\"\", \"\"],"
            + "[\"\\\"escape\\\"\", \"\\\"escape\\\"\"],"
            + "[\"\\u2605\", \"\\u263A\\u263A\"],"
            + "[\"\\ud841\\udf0e\", \"\\ud841\\udf31\\ud841\\udf79\"],"
            + "[\"{\\\"date\\\" : \\\"2017-04-28\\\",\\\"dealership\\\" : \\\"Tindel Toyota\\\"}\", \"[1,2,3,4,5]\"]";
    byte[] data = simple.getBytes(StandardCharsets.UTF_8);
    JsonResultChunk chunk = new JsonResultChunk("", 8, 2, data.length, session);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk, session);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    jp.continueParsing(byteBuffer, session);
    byte[] remaining = new byte[byteBuffer.remaining()];
    byteBuffer.get(remaining);
    jp.endParsing(ByteBuffer.wrap(remaining), session);
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
    assertEquals(
        "{\"date\" : \"2017-04-28\",\"dealership\" : \"Tindel Toyota\"}",
        chunk.getCell(7, 0).toString());
    assertEquals("[1,2,3,4,5]", chunk.getCell(7, 1).toString());
  }

  @Test
  public void simpleStreamingTest() throws SnowflakeSQLException {
    SFSession session = null;
    String simple =
        "[\"1\", \"1.01\"],"
            + "[null, null],"
            + "[\"2\", \"0.13\"],"
            + "[\"\", \"\"],"
            + "[\"\\\"escape\\\"\", \"\\\"escape\\\"\"],"
            + "[\"☺☺\", \"☺☺☺\"], "
            + "[\"\\ud841\\udf0e\", \"\\ud841\\udf31\\ud841\\udf79\"],"
            + "[\"{\\\"date\\\" : \\\"2017-04-28\\\",\\\"dealership\\\" : \\\"Tindel Toyota\\\"}\", \"[1,2,3,4,5]\"]";
    byte[] data = simple.getBytes(StandardCharsets.UTF_8);
    JsonResultChunk chunk = new JsonResultChunk("", 8, 2, data.length, session);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk, session);
    int len = 15;
    ByteBuffer byteBuffer = null;
    for (int i = 0; i < data.length; i += len) {
      if (i + len < data.length) {
        byteBuffer = ByteBuffer.wrap(data, i, len);
        jp.continueParsing(byteBuffer, session);
      } else {
        byteBuffer = ByteBuffer.wrap(data, i, data.length - i);
        jp.continueParsing(byteBuffer, session);
      }
    }
    byte[] remaining = new byte[byteBuffer.remaining()];
    byteBuffer.get(remaining);
    jp.endParsing(ByteBuffer.wrap(remaining), session);

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
    assertEquals(
        "{\"date\" : \"2017-04-28\",\"dealership\" : \"Tindel Toyota\"}",
        chunk.getCell(7, 0).toString());
    assertEquals("[1,2,3,4,5]", chunk.getCell(7, 1).toString());
  }

  /**
   * Test the largest column size 16 MB
   *
   * @throws SnowflakeSQLException Will be thrown if parsing fails
   */
  @Test
  public void LargestColumnTest() throws SnowflakeSQLException {
    SFSession session = null;
    StringBuilder sb = new StringBuilder();
    StringBuilder a = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++) {
      a.append("a");
    }
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++) {
      b.append("b");
    }
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++) {
      c.append("c");
    }
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024 - 5; i += 6) {
      s.append("\\u263A");
    }
    sb.append("[\"")
        .append(a)
        .append("\",\"")
        .append(b)
        .append("\"],[\"")
        .append(c)
        .append("\",\"")
        .append(s)
        .append("\"]");

    byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
    JsonResultChunk chunk = new JsonResultChunk("", 2, 2, data.length, session);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk, session);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    jp.continueParsing(byteBuffer, session);
    byte[] remaining = new byte[byteBuffer.remaining()];
    byteBuffer.get(remaining);
    jp.endParsing(ByteBuffer.wrap(remaining), session);
    assertEquals(a.toString(), chunk.getCell(0, 0).toString());
    assertEquals(b.toString(), chunk.getCell(0, 1).toString());
    assertEquals(c.toString(), chunk.getCell(1, 0).toString());
    assertEquals(StringEscapeUtils.unescapeJava(s.toString()), chunk.getCell(1, 1).toString());
  }

  // SNOW-802910: Test to cover edge case '\u0000\u0000' where null could be dropped.
  @Test
  public void testAsciiSequential() throws SnowflakeSQLException {
    SFSession session = null;
    String ascii = "[\"\\u0000\\u0000\\u0000\"]";
    byte[] data = ascii.getBytes(StandardCharsets.UTF_8);
    JsonResultChunk chunk = new JsonResultChunk("", 1, 1, data.length, session);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk, session);

    // parse the first null
    ByteBuffer byteBuffer = ByteBuffer.wrap(data, 0, 14);
    jp.continueParsing(byteBuffer, session);
    // parse the rest of the string
    byteBuffer = ByteBuffer.wrap(data, 9, 13);
    jp.continueParsing(byteBuffer, session);
    byteBuffer = ByteBuffer.wrap(data, 15, 7);
    jp.continueParsing(byteBuffer, session);

    // finish parsing
    byte[] remaining = new byte[byteBuffer.remaining()];
    byteBuffer.get(remaining);
    jp.endParsing(ByteBuffer.wrap(remaining), session);

    // check null is not dropped
    assertEquals("00 00 00 ", stringToHex(chunk.getCell(0, 0).toString()));
  }

  // SNOW-802910: Test to cover edge case '\u0003ä\u0000' where null could be dropped.
  @Test
  public void testAsciiCharacter() throws SnowflakeSQLException {
    SFSession session = null;
    String ascii = "[\"\\u0003ä\\u0000\"]";
    byte[] data = ascii.getBytes(StandardCharsets.UTF_8);
    JsonResultChunk chunk = new JsonResultChunk("", 1, 1, data.length, session);
    ResultJsonParserV2 jp = new ResultJsonParserV2();
    jp.startParsing(chunk, session);

    // parse ETX and UTF-8 character
    ByteBuffer byteBuffer = ByteBuffer.wrap(data, 0, data.length);
    jp.continueParsing(byteBuffer, session);

    int position = byteBuffer.position();
    // try to parse null
    byteBuffer = ByteBuffer.wrap(data, position, data.length - position);
    jp.continueParsing(byteBuffer, session);

    byte[] remaining = new byte[byteBuffer.remaining()];
    byteBuffer.get(remaining);
    jp.endParsing(ByteBuffer.wrap(remaining), session);

    // Ã00 is returned
    assertEquals("03 C3 A4 00 ", stringToHex(chunk.getCell(0, 0).toString()));
  }

  public static String stringToHex(String input) {
    byte[] byteArray = input.getBytes(StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder();
    char[] hexBytes = new char[2];
    for (int i = 0; i < byteArray.length; i++) {
      hexBytes[0] = Character.forDigit((byteArray[i] >> 4) & 0xF, 16);
      hexBytes[1] = Character.forDigit((byteArray[i] & 0xF), 16);
      sb.append(hexBytes);
      sb.append(" "); // Space every two characters to make it easier to read visually
    }
    return sb.toString().toUpperCase();
  }
}
