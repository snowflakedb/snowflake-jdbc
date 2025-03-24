package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

abstract class DiagnosticCheck {
  protected final String name;
  protected final ProxyConfig proxyConf;
  private static final SFLogger logger = SFLoggerFactory.getLogger(DiagnosticCheck.class);

  abstract void doCheck(SnowflakeEndpoint snowflakeEndpoint);

  final void run(SnowflakeEndpoint snowflakeEndpoint) {
    logger.info("JDBC Diagnostics - {}: hostname: {}", this.name, snowflakeEndpoint.getHost());
    doCheck(snowflakeEndpoint);
  }

  protected DiagnosticCheck(String name, ProxyConfig proxyConf) {
    this.name = name;
    this.proxyConf = proxyConf;
  }
}
