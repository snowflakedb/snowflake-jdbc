/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Tests for SnowflakeFileTransferAgent.mimeTypeToCompressionType
 * See https://github.com/apache/tika/blob/master/tika-core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml
 * for test cases
 */
@RunWith(Parameterized.class)
public class FileUploaderMimeTypeToCompressionTypeTest
{
  private final String mimeType;
  private final SnowflakeFileTransferAgent.FileCompressionType mimeSubType;

  public FileUploaderMimeTypeToCompressionTypeTest(String mimeType, SnowflakeFileTransferAgent.FileCompressionType mimeSubType)
  {
    this.mimeType = mimeType;
    this.mimeSubType = mimeSubType;
  }

  @Parameterized.Parameters(name = "mimeType={0}, mimeSubType={1}")
  public static Collection primeNumbers()
  {
    return Arrays.asList(new Object[][]{
        {"text/csv", null},
        {"snowflake/orc", SnowflakeFileTransferAgent.FileCompressionType.ORC},
        {"snowflake/parquet", SnowflakeFileTransferAgent.FileCompressionType.PARQUET},
        {"application/zlib", SnowflakeFileTransferAgent.FileCompressionType.DEFLATE},
        {"application/x-bzip2", SnowflakeFileTransferAgent.FileCompressionType.BZIP2},
        {"application/zstd", SnowflakeFileTransferAgent.FileCompressionType.ZSTD},
        {"application/x-brotli", SnowflakeFileTransferAgent.FileCompressionType.BROTLI},
        {"application/x-lzip", SnowflakeFileTransferAgent.FileCompressionType.LZIP},
        {"application/x-lzma", SnowflakeFileTransferAgent.FileCompressionType.LZMA},
        {"application/x-xz", SnowflakeFileTransferAgent.FileCompressionType.XZ},
        {"application/x-compress", SnowflakeFileTransferAgent.FileCompressionType.COMPRESS},
        {"application/x-gzip", SnowflakeFileTransferAgent.FileCompressionType.GZIP}
    });
  }

  @Test
  public void testMimeTypeToCompressionType() throws Throwable
  {
    assertEquals(mimeSubType, SnowflakeFileTransferAgent.mimeTypeToCompressionType(mimeType));
  }
}
