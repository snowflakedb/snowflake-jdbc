package net.snowflake.client.jdbc.diagnostic;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.spi.NamingManager;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class DnsDiagnosticCheck extends DiagnosticCheck {

  private static final SFLogger logger = SFLoggerFactory.getLogger(DnsDiagnosticCheck.class);

  private final String INITIAL_DNS_CONTEXT = "com.sun.jndi.dns.DnsContextFactory";

  DnsDiagnosticCheck(ProxyConfig proxyConfig) {
    super("DNS Lookup Test", proxyConfig);
  }

  @Override
  protected void doCheck(SnowflakeEndpoint snowflakeEndpoint) {
    getCnameRecords(snowflakeEndpoint);
    getArecords(snowflakeEndpoint);
  }

  private void getCnameRecords(SnowflakeEndpoint snowflakeEndpoint) {
    String hostname = snowflakeEndpoint.getHost();
    try {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_DNS_CONTEXT);
      DirContext dirCtx = (DirContext) NamingManager.getInitialContext(env);
      Attributes attrs1 = dirCtx.getAttributes(snowflakeEndpoint.getHost(), new String[] {"CNAME"});
      NamingEnumeration<? extends Attribute> attrs = attrs1.getAll();
      StringBuilder sb = new StringBuilder();
      sb.append("\nCNAME:\n");
      while (attrs.hasMore()) {
        Attribute a = attrs.next();
        NamingEnumeration<?> values = a.getAll();
        while (values.hasMore()) {
          sb.append(values.next());
          sb.append("\n");
        }
      }
      logger.info(sb.toString());
    } catch (NamingException e) {
      logger.error("Error occurred when getting CNAME record for host " + hostname, e);
    } catch (Exception e) {
      logger.error("Unexpected error occurred when getting CNAME record for host " + hostname, e);
    }
  }

  private void getArecords(SnowflakeEndpoint snowflakeEndpoint) {
    String hostname = snowflakeEndpoint.getHost();
    try {
      InetAddress[] addresses = InetAddress.getAllByName(hostname);
      StringBuilder sb = new StringBuilder();
      sb.append("\nA Records:\n");
      for (InetAddress ip : addresses) {
        if (ip instanceof Inet4Address) {
          sb.append(ip.getHostAddress());
          sb.append("\n");
        }
        // Check if this is a private link endpoint and if the ip address
        // returned by the DNS query is a private IP address as expected.
        if (snowflakeEndpoint.isPrivateLink() && !ip.isSiteLocalAddress()) {
          logger.error(
              "Public IP address was returned for {}. Please review your DNS configurations.",
              hostname);
        }
      }
      logger.info(sb.toString());
    } catch (UnknownHostException e) {
      logger.error("DNS query failed for host: " + snowflakeEndpoint.getHost(), e);
    }
  }
}
