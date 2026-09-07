package net.snowflake.client.internal.jdbc.cloud.storage;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Fills each {@code read(byte[], off, len)} as fully as the source allows. Client-side-encrypted
 * uploads read from a {@link javax.crypto.CipherInputStream} that returns one cipher block (~512
 * bytes) per read; since the AWS SDK v2 async request body reads once per demand, coalescing those
 * into full-size reads keeps upload throughput from collapsing. {@link java.io.BufferedInputStream}
 * does not suffice: past its buffer size it passes a read straight through to the source.
 */
final class FullReadInputStream extends FilterInputStream {
  /**
   * A blocking source must not return 0 for a positive request. Tolerate a few transient zero reads
   * before failing, rather than spinning forever.
   */
  private static final int MAX_ZERO_READS = 8;

  FullReadInputStream(InputStream in) {
    super(in);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int total = 0;
    int zeroReads = 0;
    while (total < len) {
      int n = in.read(b, off + total, len - total);
      if (n < 0) {
        return total == 0 ? -1 : total;
      }
      if (n == 0) {
        // Never hand a short body to the async publisher against a known contentLength: that
        // truncates the upload. Fail loudly instead of spinning or silently under-reading.
        if (++zeroReads > MAX_ZERO_READS) {
          throw new IOException("Upload source returned no data for a positive read request");
        }
        continue;
      }
      zeroReads = 0;
      total += n;
    }
    return total;
  }
}
