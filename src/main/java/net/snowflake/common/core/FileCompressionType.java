/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */

package net.snowflake.common.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public enum FileCompressionType
{
  GZIP(".gz", "application",
       Arrays.asList("gzip", "x-gzip"), true),
  DEFLATE(".deflate", "application",
          Arrays.asList("zlib", "deflate"), true),
  RAW_DEFLATE(".raw_deflate", "application",
              Arrays.asList("raw_deflate"), true),
  BZIP2(".bz2", "application",
        Arrays.asList("bzip2", "x-bzip2", "x-bz2", "x-bzip", "bz2"), true),
  ZSTD(".zst", "application",
       Arrays.asList("zstd"), true),
  BROTLI(".br", "application",
         Arrays.asList("brotli", "x-brotli"), true),
  LZIP(".lz", "application",
       Arrays.asList("lzip", "x-lzip"), false),
  LZMA(".lzma", "application",
       Arrays.asList("lzma", "x-lzma"), false),
  LZO(".lzo", "application",
      Arrays.asList("lzop", "x-lzop"), false),
  XZ(".xz", "application",
     Arrays.asList("xz", "x-xz"), false),
  COMPRESS(".Z", "application",
           Arrays.asList("compress", "x-compress"), false),
  PARQUET(".parquet", "snowflake",
          Collections.singletonList("parquet"), true),
  ORC(".orc", "snowflake",
      Collections.singletonList("orc"), true);

  FileCompressionType(String fileExtension, String mimeType,
                      List<String> mimeSubTypes,
                      boolean isSupported)
  {
    this.fileExtension = fileExtension;
    this.mimeType = mimeType;
    this.mimeSubTypes = mimeSubTypes;
    this.supported = isSupported;
  }

  private String fileExtension;
  private String mimeType;
  private List<String> mimeSubTypes;
  private boolean supported;

  static final Map<String, FileCompressionType> mimeSubTypeToCompressionMap =
      new HashMap<String, FileCompressionType>();

  static
  {
    for (FileCompressionType compression : FileCompressionType.values())
    {
      for (String mimeSubType : compression.mimeSubTypes)
      {
        mimeSubTypeToCompressionMap.put(mimeSubType, compression);
      }
    }
  }

  /**
   * Lookup file compression type by MimeSubType
   * for example, the MimeSubType for .gz file is "application/x-gzip".
   *
   * @param mimeSubType File type's MimeSubType
   * @return if found, return the Optional of FileCompressionType object,
   *         otherwise, return Optional.empty
   */
  static public Optional<FileCompressionType> lookupByMimeSubType(
      String mimeSubType)
  {
    FileCompressionType foundType =
        mimeSubTypeToCompressionMap.get(mimeSubType);
    return (foundType == null) ? Optional.empty()
                               : Optional.of(foundType);
  }

  /**
   * Lookup file compression type by file name extension.
   * The file extension includes DOT for example ".gz"
   *
   * @param fileExtension File name extension
   * @return if found, return the Optional of FileCompressionType object,
   *         otherwise, return Optional.empty
   */
  static public Optional<FileCompressionType> lookupByFileExtension(
      String fileExtension)
  {
    for (FileCompressionType compression : FileCompressionType.values())
    {
      if (compression.fileExtension.equals(fileExtension))
      {
        return Optional.of(compression);
      }
    }
    return Optional.empty();
  }

  public boolean isSupported()
  {
    return supported;
  }

  public String getFileExtension()
  {
    return fileExtension;
  }

  public String getMimeType()
  {
    return mimeType;
  }

  public List<String> getMimeSubTypes()
  {
    return mimeSubTypes;
  }
}
