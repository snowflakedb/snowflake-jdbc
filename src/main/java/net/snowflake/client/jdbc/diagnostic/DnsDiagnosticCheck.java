package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class DnsDiagnosticCheck extends DiagnosticCheck {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(DnsDiagnosticCheck.class);
    final String name = "DNS Lookup";

    public DnsDiagnosticCheck() { super("DNS Diagnostic Test"); }

    @Override
    public void run(SnowflakeEndpoint snowflakeEndpoint){
        try {
            InetAddress[] addresses = InetAddress.getAllByName(snowflakeEndpoint.getHost());
            for (InetAddress ip : addresses) {
                logger.debug(ip.toString());
                //Check if the endpoint is a private link endpoint and if the ip address
                //returned by the DNS query is a private IP address as expected.
                if (snowflakeEndpoint.isPrivateLink() && !ip.isSiteLocalAddress()) {
                    this.success = false;
                    logger.debug("Private Link is enabled, however, IP address is not a private one. Please check DNS server settings");
                }
            }
        }catch(UnknownHostException e){
            this.success = false;
            logger.debug("DNS query failed with an UnknownHostException for host: " + snowflakeEndpoint.getHost());
            logger.debug("Please check your DNS server's settings");
        }
    }
}

    /*
    networkaddress.cache.ttl
    Indicates the caching policy for successful name lookups from the name service.
    The value is specified as as integer to indicate the number of seconds to cache the successful lookup.
    The default setting is to cache for an implementation specific period of time.
    A value of -1 indicates "cache forever".

    networkaddress.cache.negative.ttl (default: 10)
    Indicates the caching policy for un-successful name lookups from the name service.
    The value is specified as as integer to indicate the number of seconds to cache the failure for un-successful lookups.
    A value of 0 indicates "never cache". A value of -1 indicates "cache forever".
     */

