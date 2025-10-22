package net.snowflake.client.internal.common.core;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Format (encoding scheme) for binary values.
 *
 * <p>This corresponds to the BinaryFormat class in XP.
 *
 * <p>There are three formats: 1. HEX (hexadecimal encoding) 2. BASE64 (RFC 4648 Base64 encoding) 3.
 * UTF-8 or UTF8
 *
 * <p>Each kind does two things: 1. format - convert from SFBinary to String 2. parse - convert from
 * String to SFBinary
 *
 * <p>For HEX and BASE64, "format" means "encode", and "parse" (which can fail) means "decode". For
 * UTF-8, it is the reverse: "format" means to decode bytes as Unicode characters (which can fail),
 * and "parse" means to encode a string of as UTF-8 bytes (which always suceeds).
 *
 * @author mkember
 */
public enum SFBinaryFormat {
  HEX {
    @Override
    public String format(SFBinary binary) {
      return binary.toHex();
    }

    @Override
    public SFBinary parse(String string) {
      return SFBinary.fromHex(string);
    }
  },

  BASE64 {
    @Override
    public String format(SFBinary binary) {
      return binary.toBase64();
    }

    @Override
    public SFBinary parse(String string) {
      return SFBinary.fromBase64(string);
    }
  },

  UTF8 {
    @Override
    public String format(SFBinary binary) {
      final CharBuffer buf;
      try {
        buf = UTF8_DECODER.decode(ByteBuffer.wrap(binary.getBytes()));
      } catch (CharacterCodingException ex) {
        throw new IllegalArgumentException("Invalid UTF-8");
      }
      return buf.toString();
    }

    @Override
    public SFBinary parse(String string) {
      return new SFBinary(string.getBytes(StandardCharsets.UTF_8));
    }
  };

  /**
   * Format a binary value as a string.
   *
   * @param binary SFBinary
   * @return formatted binary value
   * @throws IllegalArgumentException if the binary cannot be formatted.
   */
  public abstract String format(SFBinary binary);

  /**
   * Parse a binary value as a string.
   *
   * @param string a string
   * @return SFBinary instance
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public abstract SFBinary parse(String string);

  /**
   * The default binary format.
   *
   * <p>NOTE: Keep this in sync with BinaryFormat::DEFAULT in XP.
   */
  public static final SFBinaryFormat DEFAULT = HEX;

  /** Decoder object used for parsing and formatting UTF-8. */
  private static final CharsetDecoder UTF8_DECODER =
      StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);

  /**
   * Looks up a binary format by case-insensitive name. HEX, BASE64 or UTF-8
   *
   * @param name binary format
   * @return SFBinaryFormat instance
   * @throws IllegalArgumentException if the name is invalid.
   */
  public static SFBinaryFormat getFormat(String name) {
    try {
      return lookup(name);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Must be 'HEX', 'BASE64', or 'UTF-8'");
    }
  }

  /**
   * Looks up a binary format suitable for text output.
   *
   * <p>Specifically, does not allow UTF-8 because not all binary values can be formatted with the
   * UTF-8 format.
   *
   * @param name binary format
   * @return SFBinaryFormat instance
   * @throws IllegalArgumentException if the name is invalid.
   */
  public static SFBinaryFormat getSafeOutputFormat(String name) {
    try {
      SFBinaryFormat fmt = lookup(name);
      if (fmt == UTF8) {
        throw new IllegalArgumentException();
      }
      return fmt;
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Must be 'HEX' or 'BASE64'");
    }
  }

  /**
   * Helper function to look up a binary format by case-insensitive name.
   *
   * @param name binary format name
   * @return SFBinaryFormat instance
   * @throws IllegalArgumentException if the name is invalid.
   */
  private static SFBinaryFormat lookup(String name) {
    name = name.toUpperCase();
    if (name.equals("UTF-8")) {
      return UTF8;
    }
    return SFBinaryFormat.valueOf(name);
  }
}
