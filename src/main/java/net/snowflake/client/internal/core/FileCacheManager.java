package net.snowflake.client.internal.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.function.Supplier;

interface FileCacheManager {

  String getCacheFilePath();

  void overrideCacheFile(File newCacheFile);

  <T> T withLock(Supplier<T> supplier);

  JsonNode readCacheFile();

  void writeCacheFile(JsonNode input);

  void deleteCacheFile();
}
