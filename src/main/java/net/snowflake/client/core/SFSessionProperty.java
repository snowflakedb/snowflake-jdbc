/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.security.PrivateKey;
import java.util.regex.Pattern;
import net.snowflake.client.jdbc.ErrorCode;

/**
 * session properties accepted for opening a new session.
 *
 * @author jhuang
 *     <p>Created on 11/3/15
 */
public enum SFSessionProperty {
  SERVER_URL("serverURL", true, String.class),
  USER("user", false, String.class),
  PASSWORD("password", false, String.class),
  ACCOUNT("account", true, String.class),
  DATABASE("database", false, String.class, "db"),
  SCHEMA("schema", false, String.class),
  PASSCODE_IN_PASSWORD("passcodeInPassword", false, Boolean.class),
  PASSCODE("passcode", false, String.class),
  TOKEN("token", false, String.class),
  ID_TOKEN_PASSWORD("id_token_password", false, String.class),
  ROLE("role", false, String.class),
  AUTHENTICATOR("authenticator", false, String.class),
  OKTA_USERNAME("oktausername", false, String.class),
  PRIVATE_KEY("privateKey", false, PrivateKey.class),
  WAREHOUSE("warehouse", false, String.class),
  LOGIN_TIMEOUT("loginTimeout", false, Integer.class),
  NETWORK_TIMEOUT("networkTimeout", false, Integer.class),
  INJECT_SOCKET_TIMEOUT("injectSocketTimeout", false, Integer.class),
  INJECT_CLIENT_PAUSE("injectClientPause", false, Integer.class),
  APP_ID("appId", false, String.class),
  APP_VERSION("appVersion", false, String.class),
  OCSP_FAIL_OPEN("ocspFailOpen", false, Boolean.class),
  INSECURE_MODE("insecureMode", false, Boolean.class),
  QUERY_TIMEOUT("queryTimeout", false, Integer.class),
  STRINGS_QUOTED("stringsQuotedForColumnDef", false, Boolean.class),
  APPLICATION("application", false, String.class),
  TRACING("tracing", false, String.class),
  DISABLE_SOCKS_PROXY("disableSocksProxy", false, Boolean.class),
  // connection proxy
  USE_PROXY("useProxy", false, Boolean.class),
  PROXY_HOST("proxyHost", false, String.class),
  PROXY_PORT("proxyPort", false, String.class),
  PROXY_USER("proxyUser", false, String.class),
  PROXY_PASSWORD("proxyPassword", false, String.class),
  NON_PROXY_HOSTS("nonProxyHosts", false, String.class),
  PROXY_PROTOCOL("proxyProtocol", false, String.class),
  VALIDATE_DEFAULT_PARAMETERS("validateDefaultParameters", false, Boolean.class),
  INJECT_WAIT_IN_PUT("inject_wait_in_put", false, Integer.class),
  PRIVATE_KEY_FILE("private_key_file", false, String.class),
  PRIVATE_KEY_FILE_PWD("private_key_file_pwd", false, String.class),
  CLIENT_INFO("snowflakeClientInfo", false, String.class),
  ALLOW_UNDERSCORES_IN_HOST("allowUnderscoresInHost", false, Boolean.class);

  // property key in string
  private String propertyKey;

  // if required  when establishing connection
  private boolean required;

  // value type
  private Class<?> valueType;

  // alias to property key
  private String[] aliases;

  // application name matcher
  public static Pattern APPLICATION_REGEX = Pattern.compile("^[A-Za-z][A-Za-z0-9\\.\\-_]{1,50}$");

  public boolean isRequired() {
    return required;
  }

  public String getPropertyKey() {
    return propertyKey;
  }

  public Class<?> getValueType() {
    return valueType;
  }

  SFSessionProperty(String propertyKey, boolean required, Class<?> valueType, String... aliases) {
    this.propertyKey = propertyKey;
    this.required = required;
    this.valueType = valueType;
    this.aliases = aliases;
  }

  static SFSessionProperty lookupByKey(String propertyKey) {
    for (SFSessionProperty property : SFSessionProperty.values()) {
      if (property.propertyKey.equalsIgnoreCase(propertyKey)) {
        return property;
      } else {
        for (String alias : property.aliases) {
          if (alias.equalsIgnoreCase(propertyKey)) {
            return property;
          }
        }
      }
    }
    return null;
  }

  /**
   * Check if property value is desired class. Convert if possible
   *
   * @param property The session property to check
   * @param propertyValue The property value to check
   * @return The checked property value
   * @throws SFException Will be thrown if an invalid property value is passed in
   */
  static Object checkPropertyValue(SFSessionProperty property, Object propertyValue)
      throws SFException {
    if (propertyValue == null) {
      return null;
    }

    if (property.getValueType().isAssignableFrom(propertyValue.getClass())) {
      switch (property) {
        case APPLICATION:
          if (APPLICATION_REGEX.matcher((String) propertyValue).find()) {
            return propertyValue;
          } else {
            throw new SFException(ErrorCode.INVALID_PARAMETER_VALUE, propertyValue, property);
          }
        default:
          return propertyValue;
      }
    } else {
      if (property.getValueType() == Boolean.class && propertyValue instanceof String) {
        return SFLoginInput.getBooleanValue(propertyValue);
      } else if (property.getValueType() == Integer.class && propertyValue instanceof String) {
        return Integer.valueOf((String) propertyValue);
      }
    }

    throw new SFException(
        ErrorCode.INVALID_PARAMETER_TYPE,
        propertyValue.getClass().getName(),
        property.getValueType().getName());
  }
}
