/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class URLUtil {

    static final SFLogger logger = SFLoggerFactory.getLogger(URLUtil.class);
    public static boolean isValidURL(String url) {
        try {
            new URL(url).toURI();
            return true;
        } catch(MalformedURLException mex) {
            logger.debug("Invalid URL found - ", url);
            return false;
        } catch (URISyntaxException uex) {
            logger.debug("Invalid URL found - ", url);
            return false;
        }
    }

    @Nullable
    public static String urlEncode(String url) throws UnsupportedEncodingException {
        String encodedURL;
        try {
            encodedURL = URLEncoder.encode(url,
                    StandardCharsets.UTF_8.toString());
        } catch(UnsupportedEncodingException uex) {
            return null;
        }
        return encodedURL;
    }
}
