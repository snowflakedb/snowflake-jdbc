package net.snowflake.client.jdbc;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.common.core.SqlState;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 *
 * <p>This is the Java version of the ODBC's ResultJsonParserV2 class
 */
public class ResultJsonParserV2 {

  private enum State {
    UNINITIALIZED, // no parsing in progress
    NEXT_ROW, // Waiting for [ to start the next row
    ROW_FINISHED, // Waiting for , to separate the next row
    WAIT_FOR_VALUE, // Waiting for the next value to start
    IN_VALUE, // Copy the value and wait for its end
    IN_STRING, // Copy the string and wait for its end
    ESCAPE, // Expect escaped character next
    WAIT_FOR_NEXT // Wait for , to separate next column
  }

  private static final byte[] BNULL = {0x6e, 0x75, 0x6c, 0x6c};
  private State state = State.UNINITIALIZED;
  private int currentColumn;
  private int outputCurValuePosition;
  private int outputPosition;

  // Temporarily store unicode escape sequence when buffer is empty
  // contains \\u as well
  private ByteBuffer partialEscapedUnicode;

  private int outputDataLength;
  //  private int currentRow;
  private JsonResultChunk resultChunk;

  public void startParsing(JsonResultChunk resultChunk, SFBaseSession session)
      throws SnowflakeSQLException {
    this.resultChunk = resultChunk;
    if (state != State.UNINITIALIZED) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Json parser is already used!");
    }
    state = State.NEXT_ROW;
    outputPosition = 0;
    outputCurValuePosition = 0;
    if (partialEscapedUnicode == null) {
      partialEscapedUnicode = ByteBuffer.wrap(new byte[256]);
    } else {
      ((Buffer) partialEscapedUnicode).clear();
    }
    currentColumn = 0;

    // outputDataLength can be smaller as no ',' and '[' are stored
    outputDataLength = resultChunk.computeCharactersNeeded();
  }

  /**
   * Check if the chunk has been parsed correctly. After calling this it is safe to acquire the
   * output data
   */
  public void endParsing(SFBaseSession session) throws SnowflakeSQLException {
    if (((Buffer) partialEscapedUnicode).position() > 0) {
      ((Buffer) partialEscapedUnicode).flip();
      continueParsingInternal(partialEscapedUnicode, true, session);
      ((Buffer) partialEscapedUnicode).clear();
    }

    if (state != State.ROW_FINISHED) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "SFResultJsonParser2Failed: Chunk is truncated!");
    }
    currentColumn = 0;
    state = State.UNINITIALIZED;
  }

  /**
   * Continue parsing with the given data
   *
   * @param in readOnly byteBuffer backed by an array (the data to be reed is from position to
   *     limit)
   */
  public void continueParsing(ByteBuffer in, SFBaseSession session) throws SnowflakeSQLException {
    if (state == State.UNINITIALIZED) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Json parser hasn't been initialized!");
    }

    // If stopped during a \\u, continue here
    if (((Buffer) partialEscapedUnicode).position() > 0) {
      int lenToCopy = Math.min(12 - ((Buffer) partialEscapedUnicode).position(), in.remaining());
      if (lenToCopy > partialEscapedUnicode.remaining()) {
        resizePartialEscapedUnicode(lenToCopy);
      }
      partialEscapedUnicode.put(in.array(), in.arrayOffset() + ((Buffer) in).position(), lenToCopy);
      ((Buffer) in).position(((Buffer) in).position() + lenToCopy);

      if (((Buffer) partialEscapedUnicode).position() < 12) {
        // Not enough data to parse escaped unicode
        return;
      }
      ByteBuffer toBeParsed = partialEscapedUnicode.duplicate();
      ((Buffer) toBeParsed).flip();
      continueParsingInternal(toBeParsed, false, session);
      ((Buffer) partialEscapedUnicode).clear();
    }
    continueParsingInternal(in, false, session);
  }

  private void resizePartialEscapedUnicode(int lenToCopy) {
    int newSize = 2 * partialEscapedUnicode.capacity();
    while (newSize < partialEscapedUnicode.capacity() + lenToCopy) {
      newSize *= 2;
    }
    byte[] newArray = new byte[newSize];
    System.arraycopy(
        partialEscapedUnicode.array(),
        partialEscapedUnicode.arrayOffset(),
        newArray,
        0,
        ((Buffer) partialEscapedUnicode).position());
    ByteBuffer newBuf = ByteBuffer.wrap(newArray);
    ((Buffer) newBuf).position(((Buffer) partialEscapedUnicode).position());
    ((Buffer) partialEscapedUnicode).clear();
    partialEscapedUnicode = newBuf;
  }

  /**
   * @param in readOnly byteBuffer backed by an array (the data is from position to limit)
   * @param lastData If true, this signifies this is the last data in parsing
   * @throws SnowflakeSQLException Will be thrown if parsing the chunk data fails
   */
  private void continueParsingInternal(ByteBuffer in, boolean lastData, SFBaseSession session)
      throws SnowflakeSQLException {
    /*
     * This function parses a Snowflake result chunk json, copies the data
     * to one block of memory and creates a vector of vectors with the offsets
     * and lengths. There's one vector for each column that contains all its
     * rows.
     *
     * Result json looks like this [ "text", null, "text2" ], [...
     * The parser keeps state at which element it currently is.
     *
     */

    while (in.hasRemaining()) {
      if (outputPosition >= outputDataLength) {
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "column chunk longer than expected");
      }
      switch (state) {
        case UNINITIALIZED:
          throw new SnowflakeSQLLoggedException(
              session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "parser is in inconsistent state");
        case NEXT_ROW:
          switch (in.get()) {
            case 0x20: // ' '
            case 0x9: // '\t'
            case 0xa: // '\n'
            case 0xd: // '\r\
              // skip the whitespaces
              break;
            case 0x5b: // '['
              // beginning of the next row
              state = State.WAIT_FOR_VALUE;
              break;
            default:
              {
                throw new SnowflakeSQLLoggedException(
                    session,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    String.format(
                        "encountered unexpected character 0x%x between rows",
                        in.get(((Buffer) in).position() - 1)));
              }
          }
          break;
        case ROW_FINISHED:
          switch (in.get()) {
            case 0x2c: // ','
              state = State.NEXT_ROW;
              break;
            case 0x20: // ' '
            case 0x9: // '\t'
            case 0xa: // '\n'
            case 0xd: // '\r\
              // skip the whitespaces
              break;
            default:
              {
                throw new SnowflakeSQLLoggedException(
                    session,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    String.format(
                        "encountered unexpected character 0x%x after array",
                        in.get(((Buffer) in).position() - 1)));
              }
          }
          break;
        case WAIT_FOR_VALUE:
          switch (in.get()) {
            case 0x20: // ' '
            case 0x9: // '\t'
            case 0xa: // '\n'
            case 0xd: // '\r\
              // skip the whitespaces
              break;
            case 0x2c: // ','
              // null value
              addNullValue();
              state = State.WAIT_FOR_NEXT;
              // reread the comma in the WAIT_FOR_NEXT state
              ((Buffer) in).position(((Buffer) in).position() - 1);
              continue;
            case 0x5d: // ']'
              // null value (only saw spaces)
              addNullValue();
              currentColumn = 0;
              state = State.ROW_FINISHED;
              break;
            case 0x22: // '"'
              outputCurValuePosition = outputPosition;
              // String starts, we do not copy the parenthesis
              resultChunk.addOffset(outputPosition);
              state = State.IN_STRING;
              break;
            default:
              outputCurValuePosition = outputPosition;
              // write
              resultChunk.addOffset(outputPosition);
              addByteToOutput(in.get(((Buffer) in).position() - 1));

              state = State.IN_VALUE;
              break;
          }
          break;
        case IN_VALUE:
          switch (in.get()) {
            case 0x20: // ' '
            case 0x9: // '\t'
            case 0xa: // '\n'
            case 0xd: // '\r\
            case 0x2c: // ','
            case 0x5d: // ']'
              {
                // value ended
                int length = outputPosition - outputCurValuePosition;

                // Check if value is null
                if (length == 4 && isNull()) {
                  resultChunk.setIsNull();
                  outputPosition = outputCurValuePosition;
                } else {
                  resultChunk.setLastLength(length);
                }
                state = State.WAIT_FOR_NEXT;
                ((Buffer) in).position(((Buffer) in).position() - 1);
                continue; // reread this char in WAIT_FOR_NEXT
              }
            default:
              addByteToOutput(in.get(((Buffer) in).position() - 1));
              break;
          }
          break;
        case IN_STRING:
          switch (in.get()) {
            case 0x22: // '"'
              resultChunk.setLastLength(outputPosition - outputCurValuePosition);
              state = State.WAIT_FOR_NEXT;
              break;
            case 0x5c: // '\\'
              state = State.ESCAPE;
              break;
            default:
              // Check how many characters don't have escape characters
              // copy those with one memcpy
              int inputPositionStart = ((Buffer) in).position() - 1;
              while (in.hasRemaining()) {
                byte cur = in.get();

                if (cur == 0x22 /* '"' */ || cur == 0x5c /* '\\' */) {
                  // end of string chunk
                  ((Buffer) in).position(((Buffer) in).position() - 1);
                  break;
                }
              }

              addByteArrayToOutput(
                  in.array(),
                  in.arrayOffset() + inputPositionStart,
                  ((Buffer) in).position() - inputPositionStart);

              if (in.hasRemaining()
                  && (in.get(((Buffer) in).position()) == 0x22 /* '"' */
                      || in.get(((Buffer) in).position()) == 0x5c /* '\\' */)) {
                // Those need special parsing
                continue;
              }
          }
          break;
        case ESCAPE:
          switch (in.get()) {
            case 0x22 /* '"' */:
              addByteToOutput((byte) 0x22);
              state = State.IN_STRING;
              break;
            case 0x5c /* '\\' */:
              addByteToOutput((byte) 0x5c /* '\\' */);
              state = State.IN_STRING;
              break;
            case 0x2f: // '/'
              addByteToOutput((byte) 0x2f);
              state = State.IN_STRING;
              break;
            case 0x62: // 'b'
              addByteToOutput((byte) 0x0b /*'\b'*/);
              state = State.IN_STRING;
              break;
            case 0x66: // 'f'
              addByteToOutput((byte) 0x0c /*'\f'*/);
              state = State.IN_STRING;
              break;
            case 0x6e: // 'n'
              addByteToOutput((byte) 0xa /* '\n' */);
              state = State.IN_STRING;
              break;
            case 0x72: // 'r'
              addByteToOutput((byte) 0xd /*'\r'*/);
              state = State.IN_STRING;
              break;
            case 0x74: // 't'
              addByteToOutput((byte) 0x9 /*'\t'*/);
              state = State.IN_STRING;
              break;
            case 0x75: // 'u'
              // UTF-16 hex encoded, can be up to 12 bytes
              // when in doesn't have that many left, cache them and parse at the
              // next invocation of continueParsing()

              // have to have at least 4+2+4=10 chars left to read
              // already saw "\\u", now missing "AAAA\\uAAAA"
              if (in.remaining() >= 9 || (lastData && in.remaining() >= 3)) {
                if (!parseCodepoint(in)) {
                  throw new SnowflakeSQLLoggedException(
                      session,
                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                      SqlState.INTERNAL_ERROR,
                      "SFResultJsonParser2Failed: invalid escaped unicode character");
                }
                state = State.IN_STRING;
              } else {
                // store until next data arrives
                if (partialEscapedUnicode.remaining() < in.remaining() + 2) {
                  resizePartialEscapedUnicode(in.remaining() + 2);
                }
                partialEscapedUnicode.put((byte) 0x5c /* '\\' */);
                partialEscapedUnicode.put(
                    in.array(),
                    in.arrayOffset() + ((Buffer) in).position() - 1,
                    in.remaining() + 1);

                ((Buffer) in).position(((Buffer) in).position() + in.remaining());
                state = State.IN_STRING;
                return;
              }
              break;
            default:
              {
                throw new SnowflakeSQLLoggedException(
                    session,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    "SFResultJsonParser2Failed: encountered unexpected escape character " + "0x%x",
                    in.get(((Buffer) in).position() - 1));
              }
          }
          break;
        case WAIT_FOR_NEXT:
          switch (in.get()) {
            case 0x2c: // ',':
              ++currentColumn;
              resultChunk.nextIndex();
              if (currentColumn >= resultChunk.getColCount()) {
                throw new SnowflakeSQLLoggedException(
                    session,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    "SFResultJsonParser2Failed: Too many columns!");
              }
              state = State.WAIT_FOR_VALUE;
              break;
            case 0x5d: // ']'
              currentColumn = 0;
              resultChunk.nextIndex();
              state = State.ROW_FINISHED;
              break;
            case 0x20: // ' '
            case 0x9: // '\t'
            case 0xa: // '\n'
            case 0xd: // '\r\
              // skip whitespace
              break;
            default:
              {
                throw new SnowflakeSQLLoggedException(
                    session,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    String.format(
                        "encountered unexpected character 0x%x between columns",
                        in.get(((Buffer) in).position() - 1)));
              }
          }
          break;
      }
    }
  }

  private boolean isNull() throws SnowflakeSQLException {
    int pos = outputPosition;
    if (resultChunk.get(--pos) == BNULL[3]
        && resultChunk.get(--pos) == BNULL[2]
        && resultChunk.get(--pos) == BNULL[1]
        && resultChunk.get(--pos) == BNULL[0]) {
      return true;
    }
    return false;
  }

  private int parseQuadhex(ByteBuffer s) {
    // function from picojson
    int uni_ch = 0, hex;
    for (int i = 0; i < 4; i++) {
      if ((hex = s.get()) == -1) {
        return -1;
      }
      if (0x30 /*0*/ <= hex && hex <= 0x39 /*'9'*/) {
        hex -= 0x30 /*0*/;
      } else if (0x41 /*'A'*/ <= hex && hex <= 0x46 /*'F'*/) {
        hex -= 0x41 /*'A'*/ - 0xa;
      } else if (0x61 /*'a'*/ <= hex && hex <= 0x66 /*'f'*/) {
        hex -= 0x61 /*'a'*/ - 0xa;
      } else {
        return -1;
      }
      uni_ch = uni_ch * 16 + hex;
    }
    return uni_ch;
  }

  private void addNullValue() throws SnowflakeSQLException {
    resultChunk.addOffset(outputPosition);
  }

  private void addByteToOutput(byte c) throws SnowflakeSQLException {
    resultChunk.addByte(c, outputPosition);
    outputPosition++;
  }

  private void addByteArrayToOutput(byte[] src, int offset, int length)
      throws SnowflakeSQLException {
    resultChunk.addBytes(src, offset, outputPosition, length);
    outputPosition += length;
  }

  private boolean parseCodepoint(ByteBuffer s) throws SnowflakeSQLException {
    int uni_ch;
    if ((uni_ch = parseQuadhex(s)) == -1) {
      return false;
    }
    if (0xd800 <= uni_ch && uni_ch <= 0xdfff) {
      if (0xdc00 <= uni_ch) {
        // a second 16-bit of a surrogate pair appeared
        return false;
      }
      // first 16-bit of surrogate pair, get the next one
      if (2 >= s.remaining()) {
        // not long enough for \\u
        return false;
      }
      if (s.get() != 0x5c /* '\\' */ || s.get() != 0x75 /* 'u' */) {
        return false;
      }
      if (4 > s.remaining()) {
        // not long enough for the next four hex chars
        return false;
      }
      int second = parseQuadhex(s);
      if (!(0xdc00 <= second && second <= 0xdfff)) {
        return false;
      }
      uni_ch = ((uni_ch - 0xd800) << 10) | ((second - 0xdc00) & 0x3ff);
      uni_ch += 0x10000;
    }
    if (uni_ch < 0x80) {
      addByteToOutput((byte) uni_ch);
    } else {
      if (uni_ch < 0x800) {
        addByteToOutput((byte) (0xc0 | (uni_ch >> 6)));
      } else {
        if (uni_ch < 0x10000) {
          addByteToOutput((byte) (0xe0 | (uni_ch >> 12)));
        } else {
          addByteToOutput((byte) (0xf0 | (uni_ch >> 18)));
          addByteToOutput((byte) (0x80 | ((uni_ch >> 12) & 0x3f)));
        }
        addByteToOutput((byte) (0x80 | ((uni_ch >> 6) & 0x3f)));
      }
      addByteToOutput((byte) (0x80 | (uni_ch & 0x3f)));
    }
    return true;
  }
}
