/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class URLUtil {

  static final SFLogger logger = SFLoggerFactory.getLogger(URLUtil.class);
  static final String validURLPattern =
      "^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$";
  static final Pattern pattern = Pattern.compile(validURLPattern);

  public static boolean isValidURL(String url) {
    try {
      Matcher matcher = pattern.matcher(url);
      return matcher.find();
    } catch (PatternSyntaxException pex) {
      // Do we really need this part? Any use cases?
      logger.debug("The URL REGEX is invalid. Falling back to basic sanity test");
      try {
        new URL(url).toURI();
        return true;
      } catch (MalformedURLException mex) {
        logger.debug("The URL " + url + ", is invalid");
        return false;
      } catch (URISyntaxException uex) {
        logger.debug("The URL " + url + ", is invalid");
        return false;
      }
    }
  }

  @Nullable
  public static String urlEncode(String target) throws UnsupportedEncodingException {
    String encodedTarget;
    try {
      encodedTarget = URLEncoder.encode(target, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException uex) {
      logger.debug("The string to be encoded- " + target + ", is invalid");
      return null;
    }
    return encodedTarget;
  }
}
