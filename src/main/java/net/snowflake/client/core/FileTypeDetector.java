package net.snowflake.client.core;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.tika.Tika;

/** Use Tika to detect the mime type of files */
public class FileTypeDetector extends java.nio.file.spi.FileTypeDetector {
  private final Tika tika = new Tika();

  @Override
  public String probeContentType(Path path) throws IOException {
    return tika.detect(path.toFile());
  }
}
