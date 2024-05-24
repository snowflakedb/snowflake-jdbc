package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

public class HttpAndHttpsDiagnosticCheck extends DiagnosticCheck {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(HttpAndHttpsDiagnosticCheck.class);
    final String name = "HTTP/S Connection";
    final String HTTP_SCHEMA="http://";
    final String HTTPS_SCHEMA="https://";

    public HttpAndHttpsDiagnosticCheck() { super("HTTP/HTTPS Connection Test"); }
    @Override
    public void run(SnowflakeEndpoint snowflakeEndpoint) {
        super.run(snowflakeEndpoint);
        // We have to replace underscores with hyphens because the JDK doesn't allow underscores in the hostname
        String host = snowflakeEndpoint.getHost().replace('_','-');
        try {
            String urlString;
            urlString = (snowflakeEndpoint.isSslEnabled()) ? HTTPS_SCHEMA + host : HTTP_SCHEMA + host;
            URL url = new URL(urlString);
            HttpURLConnection con = (snowflakeEndpoint.isSslEnabled()) ? (HttpsURLConnection) url.openConnection() : (HttpURLConnection) url.openConnection();
            logger.debug("Response from server: " + con.getResponseCode() + " - " + con.getResponseMessage());
            Map<String, List<String>> headerFields = con.getHeaderFields();

            // for(Map.Entry<String, List<String>> header : headerFields.entrySet())
            //   logger.debug(header.getKey() + ": " + header.getValue());

        }catch(MalformedURLException e) {
            logger.error("The URL format is incorrect, please check your allowlist JSON file for errors.");
            logger.error(e.getMessage());
            e.printStackTrace(System.err);
        }catch(Exception e){
            logger.error(e.getMessage());
            e.printStackTrace(System.err);
            this.success = false;
        }
    }

}
