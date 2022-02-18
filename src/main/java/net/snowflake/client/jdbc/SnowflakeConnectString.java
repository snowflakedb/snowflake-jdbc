/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import com.google.common.base.Strings;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SnowflakeConnectString implements Serializable {
  private static final long serialVersionUID = 1L;
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeConnectString.class);

  private final String scheme;
  private final String host;
  private final int port;
  private final Map<String, Object> parameters;
  private final String account;

  private static SnowflakeConnectString INVALID_CONNECT_STRING =
      new SnowflakeConnectString("", "", -1, Collections.emptyMap(), "");

  private static final String PREFIX = "jdbc:snowflake://";

  public static SnowflakeConnectString parse(String url, Properties info) {
    if (url == null) {
      return INVALID_CONNECT_STRING;
    }
    int pos = url.indexOf(PREFIX);
    if (pos != 0) {
      return INVALID_CONNECT_STRING; // not start with jdbc:snowflake://
    }
    String afterPrefix = url.substring(pos + PREFIX.length());
    String scheme;
    String host = null;
    int port = -1;
    Map<String, Object> parameters = new HashMap<>();
    try {
      URI uri;

      if (!afterPrefix.startsWith("http://") && !afterPrefix.startsWith("https://")) {
        // not explicitly specified
        afterPrefix = url.substring(url.indexOf("snowflake:"));
      }
      uri = new URI(afterPrefix);
      scheme = uri.getScheme();
      String authority = uri.getRawAuthority();
      String[] hostAndPort = authority.split(":");
      if (hostAndPort.length == 2) {
        host = hostAndPort[0];
        port = Integer.parseInt(hostAndPort[1]);
      } else if (hostAndPort.length == 1) {
        host = hostAndPort[0];
      }
      String queryData = uri.getRawQuery();

      if (!scheme.equals("snowflake") && !scheme.equals("http") && !scheme.equals("https")) {
        return INVALID_CONNECT_STRING;
      }
      if (Strings.isNullOrEmpty(host)) {
        return INVALID_CONNECT_STRING;
      }
      if (port == -1) {
        port = 443;
      }
      String path = uri.getPath();
      if (!Strings.isNullOrEmpty(path) && !"/".equals(path)) {
        return INVALID_CONNECT_STRING;
      }
      String account = null;
      if (!Strings.isNullOrEmpty(queryData)) {
        String[] params = queryData.split("&");
        for (String p : params) {
          String[] keyVals = p.split("=");
          if (keyVals.length != 2) {
            continue; // ignore invalid pair of parameters.
          }
          try {
            String k = URLDecoder.decode(keyVals[0], "UTF-8");
            String v = URLDecoder.decode(keyVals[1], "UTF-8");
            if ("ssl".equalsIgnoreCase(k) && !getBooleanTrueByDefault(v)) {
              scheme = "http";
            } else if ("account".equalsIgnoreCase(k)) {
              account = v;
            }
            parameters.put(k.toUpperCase(Locale.US), v);
          } catch (UnsupportedEncodingException ex0) {
            logger.info("Failed to decode a parameter {}. Ignored.", p);
          }
        }
      }
      if ("snowflake".equals(scheme)) {
        scheme = "https"; // by default
      }

      if (info.size() > 0) {
        // NOTE: value in info could be any data type.
        // overwrite the properties
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
          String k = entry.getKey().toString();
          Object v = entry.getValue();
          if ("ssl".equalsIgnoreCase(k) && !getBooleanTrueByDefault(v)) {
            scheme = "http";
          } else if ("account".equalsIgnoreCase(k)) {
            account = (String) v;
          }
          parameters.put(k.toUpperCase(Locale.US), v);
        }
      }

      if (parameters.get("ACCOUNT") == null && account == null && host.indexOf(".") > 0) {
        account = host.substring(0, host.indexOf("."));
        // If this is a global URL, then extract out the external ID part
        if (host.contains(".global.")) {
          account = account.substring(0, account.lastIndexOf('-'));
        }
        // Account names should not be altered. Set it to a value without org name
        // if it's a global url
        parameters.put("ACCOUNT", account);
      }

      if (account.contains("_")
          && parameters.containsKey(
              SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey().toUpperCase())
          && "false"
              .equalsIgnoreCase(
                  (String)
                      parameters.get(
                          SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST
                              .getPropertyKey()
                              .toUpperCase()))
          && host.startsWith(account)) {
        // The account needs to have underscores in it and the host URL needs to start
        // with the account name. There are cases where the host URL might not have the
        // the account name in it, ex - ip address instead of host name.
        // Update the Host URL to remove underscores if there are any
        String account_wo_uscores = account.replaceAll("_", "-");
        host = host.replaceFirst(account, account_wo_uscores);
      }

      return new SnowflakeConnectString(scheme, host, port, parameters, account);
    } catch (Exception ex) {
      return INVALID_CONNECT_STRING;
    }
  }

  private SnowflakeConnectString(
      String scheme, String host, int port, Map<String, Object> parameters, String account) {
    this.scheme = scheme;
    this.host = host;
    this.port = port;
    this.parameters = parameters;
    this.account = account;
  }

  public String toString() {
    return toString(true);
  }

  public String toString(boolean maskSensitiveValue) {
    StringBuilder urlStr = new StringBuilder();
    urlStr.append(scheme);
    urlStr.append("://");
    urlStr.append(host);
    urlStr.append(":");
    urlStr.append(port);

    urlStr.append(parameters.size() > 0 ? "?" : "");

    int cnt = 0;
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      if (cnt > 0) {
        urlStr.append('&');
      }
      try {
        String k = URLEncoder.encode(entry.getKey(), "UTF-8");
        String v = URLEncoder.encode(entry.getValue().toString(), "UTF-8");
        urlStr.append(k).append('=');
        if (maskSensitiveValue && "password".equalsIgnoreCase(k)
            || "passcode".equalsIgnoreCase(k)
            || "proxyPassword".equalsIgnoreCase(k)) {
          urlStr.append("******");
        } else {
          urlStr.append(v);
        }
      } catch (UnsupportedEncodingException ex) {
        logger.info("Failed to encode a parameter {}. Ignored.", entry.getKey());
      }
      ++cnt;
    }
    return urlStr.toString();
  }

  public boolean isValid() {
    // invalid if host name is null or empty
    return !Strings.isNullOrEmpty(host);
  }

  public String getScheme() {
    return scheme;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public String getAccount() {
    return account;
  }

  private static boolean getBooleanTrueByDefault(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    String vs = value.toString();
    return !"off".equalsIgnoreCase(vs) && !Boolean.FALSE.toString().equalsIgnoreCase(vs);
  }
}
