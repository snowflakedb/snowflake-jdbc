package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

abstract class DiagnosticCheck {
  protected String name;
  protected boolean success;
  protected ProxyConfig proxyConf;
  private static final SFLogger logger = SFLoggerFactory.getLogger(DiagnosticCheck.class);

  public String name() {
    return this.name;
  }

  public void run(SnowflakeEndpoint snowflakeEndpoint) {
    logger.debug("JDBC Diagnostics - {}: hostname: {}", this.name, snowflakeEndpoint.getHost());
  }

  public boolean isSuccess() {
    return this.success;
  }

  protected DiagnosticCheck(String name, ProxyConfig proxyConf) {
    this.name = name;
    this.proxyConf = proxyConf;
  }
}
