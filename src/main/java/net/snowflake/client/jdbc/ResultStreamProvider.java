package net.snowflake.client.jdbc;

import java.io.InputStream;

// Defines how the underlying data stream is to be fetched; i.e.
// allows large resultset data to come from a different source
public interface ResultStreamProvider {
  InputStream getInputStream(ChunkDownloadContext context) throws Exception;
}
