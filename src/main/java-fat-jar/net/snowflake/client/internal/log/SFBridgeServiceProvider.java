package net.snowflake.client.internal.log;

import net.snowflake.client.internal.driver.DriverVersionProperties;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * SLF4J service provider that bridges the shaded SLF4J (used internally by AWS SDK v2, Azure SDK,
 * Google Cloud SDK) to the Snowflake JDBC driver's own logging abstraction ({@link
 * SFLoggerFactory}).
 *
 * <p>This class is discovered by the shaded SLF4J's {@link java.util.ServiceLoader} mechanism. The
 * shade plugin rewrites the SPI registration file and all {@code org.slf4j} references in this
 * class to the shaded namespace, so it integrates with the shaded SLF4J — not the user's real
 * SLF4J.
 *
 * <p>This allows internal dependency logging to flow through the driver's {@link SFLoggerFactory},
 * which routes to either JUL (default) or the user's SLF4J binding (when opted in).
 */
public class SFBridgeServiceProvider implements SLF4JServiceProvider {

  private static final String REQUESTED_API_VERSION =
      DriverVersionProperties.get("slf4j.version");

  private ILoggerFactory loggerFactory;
  private IMarkerFactory markerFactory;
  private MDCAdapter mdcAdapter;

  @Override
  public ILoggerFactory getLoggerFactory() {
    return loggerFactory;
  }

  @Override
  public IMarkerFactory getMarkerFactory() {
    return markerFactory;
  }

  @Override
  public MDCAdapter getMDCAdapter() {
    return mdcAdapter;
  }

  @Override
  public String getRequestedApiVersion() {
    return REQUESTED_API_VERSION;
  }

  @Override
  public void initialize() {
    loggerFactory = new SFBridgeLoggerFactory();
    markerFactory = new BasicMarkerFactory();
    mdcAdapter = new NOPMDCAdapter();
  }
}
