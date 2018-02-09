/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.tika.Tika;

/**
 *
 * @author jhuang
 */
/**
 * Use Tika to detect the mime type of files
 */
public class FileTypeDetector extends java.nio.file.spi.FileTypeDetector
{
  private final Tika tika = new Tika();

  @Override
  public String probeContentType(Path path) throws IOException
  {
    return tika.detect(path.toFile());
  }
}
