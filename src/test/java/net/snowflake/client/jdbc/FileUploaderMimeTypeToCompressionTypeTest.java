/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import net.snowflake.common.core.FileCompressionType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for SnowflakeFileTransferAgent.mimeTypeToCompressionType See
 * https://github.com/apache/tika/blob/master/tika-core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml
 * for test cases
 */
@RunWith(Parameterized.class)
public class FileUploaderMimeTypeToCompressionTypeTest {
  private final String mimeType;
  private final FileCompressionType mimeSubType;

  public FileUploaderMimeTypeToCompressionTypeTest(
      String mimeType, FileCompressionType mimeSubType) {
    this.mimeType = mimeType;
    this.mimeSubType = mimeSubType;
  }

  @Parameterized.Parameters(name = "mimeType={0}, mimeSubType={1}")
  public static Collection primeNumbers() {
    return Arrays.asList(
        new Object[][] {
          {"text/csv", null},
          {"snowflake/orc", FileCompressionType.ORC},
          {"snowflake/orc;p=1", FileCompressionType.ORC},
          {"snowflake/parquet", FileCompressionType.PARQUET},
          {"application/zlib", FileCompressionType.DEFLATE},
          {"application/x-bzip2", FileCompressionType.BZIP2},
          {"application/zstd", FileCompressionType.ZSTD},
          {"application/x-brotli", FileCompressionType.BROTLI},
          {"application/x-lzip", FileCompressionType.LZIP},
          {"application/x-lzma", FileCompressionType.LZMA},
          {"application/x-xz", FileCompressionType.XZ},
          {"application/x-compress", FileCompressionType.COMPRESS},
          {"application/x-gzip", FileCompressionType.GZIP}
        });
  }

  @Test
  public void testMimeTypeToCompressionType() throws Throwable {
    Optional<FileCompressionType> foundCompType =
        SnowflakeFileTransferAgent.mimeTypeToCompressionType(mimeType);
    if (foundCompType.isPresent()) {
      assertEquals(mimeSubType, foundCompType.get());
    } else {
      assertEquals(mimeSubType, null);
    }
  }
}
