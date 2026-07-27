package net.snowflake.client.internal.jdbc;

import static net.snowflake.client.internal.core.Constants.MB;
import static net.snowflake.common.core.FileCompressionType.GZIP;
import static net.snowflake.common.core.FileCompressionType.ZSTD;

import com.github.luben.zstd.ZstdInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.common.core.SqlState;
import org.apache.http.Header;

class CompressedStreamFactory {

  private static final int STREAM_BUFFER_SIZE = MB;

  /**
   * Determine the format of the response, if it is not either plain text or gzip, raise an error.
   */
  public InputStream createBasedOnEncodingHeader(InputStream is, Header encoding)
      throws IOException, SnowflakeSQLException {
    if (encoding != null) {
      if (GZIP.name().equalsIgnoreCase(encoding.getValue())) {
        return new GZIPInputStream(is, STREAM_BUFFER_SIZE);
      } else if (ZSTD.name().equalsIgnoreCase(encoding.getValue())) {
        return new ZstdInputStream(is);
      } else {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Exception: unexpected compression got " + encoding.getValue());
      }
    } else {
      return DefaultResultStreamProvider.detectGzipAndGetStream(is);
    }
  }
}
