package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;

public class DnsDiagnosticCheck extends DiagnosticCheck {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(DnsDiagnosticCheck.class);

    public DnsDiagnosticCheck() { super("DNS Lookup Test"); }

    @Override
    public void run(SnowflakeEndpoint snowflakeEndpoint){
        super.run(snowflakeEndpoint);
        try {
            getCnameRecords(snowflakeEndpoint.getHost());
            InetAddress[] addresses = InetAddress.getAllByName(snowflakeEndpoint.getHost());
            for (InetAddress ip : addresses) {
                if (ip instanceof Inet4Address) {
                    logger.debug(ip.getHostAddress());
                }
                //Check if the endpoint is a private link endpoint and if the ip address
                //returned by the DNS query is a private IP address as expected.
                if (snowflakeEndpoint.isPrivateLink() && !ip.isSiteLocalAddress()) {
                    this.success = false;
                    logger.error("Public IP address was returned for a private link endpoint. Please review your DNS configurations.");
                }
            }
        }catch(UnknownHostException e){
            this.success = false;
            logger.error("DNS query failed with an UnknownHostException for host: " + snowflakeEndpoint.getHost());
            logger.error("Please check your DNS server's settings");
        }
    }

    private void getCnameRecords(String hostname) {
        try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            // TODO: Consider adding the ability to provide a particular DNS server for lookups
            // env.put("java.naming.provider.url",    "dns://8.8.8.8");

            DirContext dirCtx = new InitialDirContext(env);
            Attributes attrs1 = dirCtx.getAttributes("imwwjzsfcb1stg.blob.core.windows.net", new String[] { "CNAME"});
            NamingEnumeration<? extends Attribute> attrs = attrs1.getAll();
            while(attrs.hasMore()) {
                Attribute a = attrs.next();
                System.out.println(a.getID());
                NamingEnumeration<?> values = a.getAll();
                while (values.hasMore()) {
                    System.out.println(values.next());
                }
                System.out.println();

            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
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

