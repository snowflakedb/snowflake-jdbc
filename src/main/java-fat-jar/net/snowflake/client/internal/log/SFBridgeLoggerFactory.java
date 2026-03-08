package net.snowflake.client.internal.log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

/**
 * Logger factory for the shaded SLF4J bridge. Returns {@link SFBridgeLogger} instances that
 * delegate to {@link SFLoggerFactory}.
 */
public class SFBridgeLoggerFactory implements ILoggerFactory {

  private final ConcurrentMap<String, SFBridgeLogger> loggerMap = new ConcurrentHashMap<>();

  @Override
  public Logger getLogger(String name) {
    return loggerMap.computeIfAbsent(name, SFBridgeLogger::new);
  }
}
