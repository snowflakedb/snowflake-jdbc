package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.stream.Stream;
import net.snowflake.common.core.FileCompressionType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests for SnowflakeFileTransferAgent.mimeTypeToCompressionType See
 * https://github.com/apache/tika/blob/master/tika-core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml
 * for test cases
 */
public class FileUploaderMimeTypeToCompressionTypeTest {

  static class MimeTypesProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("text/", null),
          Arguments.of("text/csv", null),
          Arguments.of("snowflake/orc", FileCompressionType.ORC),
          Arguments.of("snowflake/orc;p=1", FileCompressionType.ORC),
          Arguments.of("snowflake/parquet", FileCompressionType.PARQUET),
          Arguments.of("application/zlib", FileCompressionType.DEFLATE),
          Arguments.of("application/x-bzip2", FileCompressionType.BZIP2),
          Arguments.of("application/zstd", FileCompressionType.ZSTD),
          Arguments.of("application/x-brotli", FileCompressionType.BROTLI),
          Arguments.of("application/x-lzip", FileCompressionType.LZIP),
          Arguments.of("application/x-lzma", FileCompressionType.LZMA),
          Arguments.of("application/x-xz", FileCompressionType.XZ),
          Arguments.of("application/x-compress", FileCompressionType.COMPRESS),
          Arguments.of("application/x-gzip", FileCompressionType.GZIP));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MimeTypesProvider.class)
  public void testMimeTypeToCompressionType(String mimeType, FileCompressionType mimeSubType)
      throws Throwable {
    Optional<FileCompressionType> foundCompType =
        SnowflakeFileTransferAgent.mimeTypeToCompressionType(mimeType);
    if (foundCompType.isPresent()) {
      assertEquals(mimeSubType, foundCompType.get());
    } else {
      assertEquals(mimeSubType, null);
    }
  }
}
