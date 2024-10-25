package net.snowflake.client.jdbc;

import static net.snowflake.client.core.Constants.MB;
import static net.snowflake.common.core.FileCompressionType.GZIP;
import static net.snowflake.common.core.FileCompressionType.ZSTD;

import com.github.luben.zstd.ZstdInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;
import net.snowflake.common.core.SqlState;
import org.apache.http.Header;

class CompressedStreamFactory {

  private static final int STREAM_BUFFER_SIZE = MB;

  public InputStream createBasedOnEncodingHeader(InputStream is, Header encoding)
      throws IOException, SnowflakeSQLException {
    InputStream inputStream = is; // Determine the format of the response, if it is not
    // either plain text or gzip, raise an error.
    if (encoding != null) {
      if (GZIP.name().equalsIgnoreCase(encoding.getValue())) {
        /* specify buffer size for GZIPInputStream */
        inputStream = new GZIPInputStream(is, STREAM_BUFFER_SIZE);
      } else if (ZSTD.name().equalsIgnoreCase(encoding.getValue())) {
        inputStream = new ZstdInputStream(is);
      } else {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Exception: unexpected compression got " + encoding.getValue());
      }
    } else {
      inputStream = detectGzipAndGetStream(is);
    }

    return inputStream;
  }

  private InputStream detectGzipAndGetStream(InputStream is) throws IOException {
    PushbackInputStream pb = new PushbackInputStream(is, 2);
    byte[] signature = new byte[2];
    int len = pb.read(signature);
    pb.unread(signature, 0, len);
    // https://tools.ietf.org/html/rfc1952
    if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
      return new GZIPInputStream(pb);
    } else {
      return pb;
    }
  }
}
