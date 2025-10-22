package net.snowflake.client.internal.common.core;

import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.codec.binary.Base16;
import org.apache.commons.codec.binary.Base64;

/**
 * Represents binary values.
 *
 * <p>Just a wrapper around a byte array.
 *
 * <p>Instances of this class are immutable.
 *
 * @author mkember
 */
public class SFBinary {
  private static final Base16 INSTANCE = new Base16();
  /** An empty SFBinary. */
  public static final SFBinary EMPTY = new SFBinary(new byte[] {});

  /**
   * Special SFBinary that is greater than all others.
   *
   * <p>Specifically, MAXIMUM.compareTo(x) &gt; 0 for all x != MAXIMUM. This value has no byte array
   * representation, so calling getBytes, toBase64, or concat on it is NOT allowed. It does,
   * however, have a special representation "Z", so SFBinary.fromHex("Z") returns MAXIMUM, and
   * MAXIMUM.toHex() returns "Z".
   */
  public static final SFBinary MAXIMUM = new SFBinary(null);

  private static final String MAXIMUM_HEX = "Z";

  // Used for validating hex-encoded strings.
  private static final byte[] HEX_TABLE;
  private static final byte INVALID = 0;
  private static final byte HEX_DIGIT = 1;
  private static final byte WHITESPACE = 2;

  static {
    // Initialize HEX_TABLE with 0-9, a-f, A-F, and whitespace.
    byte[] temp = new byte['f' + 1];
    for (char c = '0'; c <= '9'; c++) {
      temp[c] = HEX_DIGIT;
    }
    for (char c = 'A'; c <= 'F'; c++) {
      temp[c] = HEX_DIGIT;
    }
    for (char c = 'a'; c <= 'f'; c++) {
      temp[c] = HEX_DIGIT;
    }
    temp[' '] = WHITESPACE;
    temp['\n'] = WHITESPACE;
    temp['\r'] = WHITESPACE;
    HEX_TABLE = temp;
  }

  private final byte[] bytes;

  /**
   * Constructs an SFBinary from a byte array.
   *
   * @param bytes an byte array
   */
  public SFBinary(byte[] bytes) {
    this.bytes = bytes;
  }

  /**
   * Returns true if it's safe to call SFBinary.fromHex(str).
   *
   * <p>This is meant for checking user input, so it allows spaces, newlines, and carriage returns.
   * It returns false for the special value "Z".
   *
   * @param str a string
   * @return true if a string is a hexadecimal value otherwise false
   */
  public static boolean validHex(String str) {
    int count = 0;
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      int type = c < HEX_TABLE.length ? HEX_TABLE[c] : INVALID;
      if (type == INVALID) {
        return false;
      }
      if (type == HEX_DIGIT) {
        count++;
      }
    }

    return count % 2 == 0;
  }

  /**
   * Creates an SFBinary by decoding a hex-encoded string (uppercase letters).
   *
   * <p>Handles the special value "Z" by returning MAXIMUM.
   *
   * @param str a string
   * @return SFBinary
   * @throws IllegalArgumentException if the string is not hex-encoded.
   */
  public static SFBinary fromHex(String str) {
    if (str.equals(MAXIMUM_HEX)) {
      return MAXIMUM;
    }
    if (!validHex(str)) {
      throw new IllegalArgumentException("Invalid hex in '" + str + "'");
    }

    return new SFBinary(INSTANCE.decode(str));
  }

  /**
   * Creates an SFBinary by decoding a Base64-encoded string (RFC 4648).
   *
   * @param str a string.
   * @return SFBinary
   * @throws IllegalArgumentException if the string is not Base64-encoded.
   */
  public static SFBinary fromBase64(String str) {
    return new SFBinary(INSTANCE.decode(str));
  }

  /**
   * Returns the underlying byte array.
   *
   * @return a byte array
   */
  public byte[] getBytes() {
    assert !this.equals(MAXIMUM);
    return bytes;
  }

  /**
   * Returns the length of the SFBinary in bytes.
   *
   * @return byte length
   */
  public int length() {
    return bytes.length;
  }

  /**
   * Encodes the binary value as a hex string (uppercase letters).
   *
   * <p>Handles MAXIMUM by returning the special value "Z".
   *
   * @return a hexdecimal string
   */
  public String toHex() {
    if (this.equals(MAXIMUM)) {
      return MAXIMUM_HEX;
    }

    return INSTANCE.encodeAsString(bytes);
  }

  /**
   * Encodes the binary value as a Base64 string (RFC 4648).
   *
   * @return a base64 string
   */
  public String toBase64() {
    assert !this.equals(MAXIMUM);
    return Base64.encodeBase64String(bytes);
  }

  /**
   * Returns a new SFBinary that is a substring of this SFBinary.
   *
   * <p>Same semantics as String.substring: 'start' is inclusive, 'end' is exclusive, and 'start'
   * cannot be greater than 'end'.
   *
   * @param start the starting index of byte array
   * @param end the ending index of byte array
   * @return SFBinary
   */
  public SFBinary substring(int start, int end) {
    if (start == end) {
      return EMPTY;
    }

    return new SFBinary(Arrays.copyOfRange(bytes, start, end));
  }

  /**
   * Concatenates two binary values.
   *
   * <p>Concatenates the bytes of this SFBinary with the bytes of the other SFBinary, returning a
   * new SFBinary instance.
   *
   * @param other SFBinary to append
   * @return concatenated SFBinary
   */
  public SFBinary concat(SFBinary other) {
    assert !this.equals(MAXIMUM);
    assert !Objects.equals(other, MAXIMUM);

    byte[] result = Arrays.copyOf(bytes, bytes.length + other.bytes.length);
    System.arraycopy(
        other.bytes,
        0, // source
        result,
        bytes.length, // destination
        other.bytes.length); // length
    return new SFBinary(result);
  }

  /**
   * Compares SFBinary
   *
   * @param other the target SFBinary
   * @return 1 if this SFBinary is larger, 0 if identical otherwise -1
   */
  public int compareTo(SFBinary other) {
    if (this.equals(MAXIMUM) && Objects.equals(other, MAXIMUM)) {
      // This logic is correct for most cases. For example, when choosing the
      // larger of two EP maxes, and both are Z, it doesn't matter what this
      // returns. It *would* be a problem if min and max were both Z and this
      // returned 0 (implying the expression is constant), but that comparison
      // will never happen, because XP never produces Z for the min.
      return 0;
    }
    if (this.equals(MAXIMUM)) {
      return 1;
    } else if (Objects.equals(other, MAXIMUM)) {
      return -1;
    }

    // Compare the byte arrays lexicographically.
    for (int i = 0; i < bytes.length && i < other.bytes.length; i++) {
      int a = bytes[i] & 0xFF;
      int b = other.bytes[i] & 0xFF;
      if (a > b) {
        return 1;
      } else if (a < b) {
        return -1;
      }
    }

    return bytes.length - other.bytes.length;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    if (this.equals(MAXIMUM)) {
      return 0;
    }

    return Arrays.hashCode(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (this == MAXIMUM || other == MAXIMUM) {
      return this == MAXIMUM && other == MAXIMUM;
    }

    return other instanceof SFBinary && Arrays.equals(bytes, ((SFBinary) other).bytes);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "SFBinary(hex=" + toHex() + ")";
  }
}
