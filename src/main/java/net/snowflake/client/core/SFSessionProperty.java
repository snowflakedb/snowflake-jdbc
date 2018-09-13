/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/**
 * Created by jhuang on 11/3/15.
 */

import net.snowflake.client.jdbc.ErrorCode;

import java.security.PrivateKey;
import java.util.regex.Pattern;

/**
 * session properties accepted for opening a new session.
 */
public enum SFSessionProperty
{
  SERVER_URL("serverURL", true, String.class),
  USER("user", false, String.class),
  PASSWORD("password", false, String.class),
  ACCOUNT("account", true, String.class),
  DATABASE("database", false, String.class, "db"),
  SCHEMA("schema", false, String.class),
  PASSCODE_IN_PASSWORD("passcodeInPassword", false, Boolean.class),
  PASSCODE("passcode", false, String.class),
  TOKEN("token", false, String.class),
  ID_TOKEN("id_token", false, String.class),
  ID_TOKEN_PASSWORD("id_token_password", false, String.class),
  ROLE("role", false, String.class),
  AUTHENTICATOR("authenticator", false, String.class),
  PRIVATE_KEY("privateKey", false, PrivateKey.class),
  WAREHOUSE("warehouse", false, String.class),
  LOGIN_TIMEOUT("loginTimeout", false, Integer.class),
  NETWORK_TIMEOUT("networkTimeout", false, Integer.class),
  INJECT_SOCKET_TIMEOUT("injectSocketTimeout", false, Integer.class),
  INJECT_CLIENT_PAUSE("injectClientPause", false, Integer.class),
  APP_ID("appId", false, String.class),
  APP_VERSION("appVersion", false, String.class),
  INSECURE_MODE("insecureMode", false, Boolean.class),
  QUERY_TIMEOUT("queryTimeout", false, Integer.class),
  APPLICATION("application", false, String.class),
  TRACING("tracing", false, String.class),
  DISABLE_SOCKS_PROXY("disableSocksProxy", false, Boolean.class),
  ;

  // property key in string
  private String propertyKey;

  // if required  when establishing connection
  private boolean required;

  // value type
  private Class valueType;

  // alias to property key
  private String[] aliases;

  // application name matcher
  static Pattern APPLICATION_REGEX = Pattern.compile("^[A-Za-z][A-Za-z0-9\\.\\-_]{1,50}$");

  public boolean isRequired()
  {
    return required;
  }

  public String getPropertyKey()
  {
    return propertyKey;
  }

  public Class getValueType()
  {
    return valueType;
  }

  SFSessionProperty(String propertyKey,
                    boolean required,
                    Class valueType,
                    String... aliases)
  {
    this.propertyKey = propertyKey;
    this.required = required;
    this.valueType = valueType;
    this.aliases = aliases;
  }

  static SFSessionProperty lookupByKey(String propertyKey)
  {
    for (SFSessionProperty property : SFSessionProperty.values())
    {
      if (property.propertyKey.equalsIgnoreCase(propertyKey))
      {
        return property;
      }
      else
      {
        for(String alias : property.aliases)
        {
          if (alias.equalsIgnoreCase(propertyKey))
          {
            return property;
          }
        }
      }
    }
    return  null;
  }

  /**
   * Check if property value is desired class. Convert if possible
   * @param property
   * @param propertyValue
   * @return
   * @throws SFException
   */
  static Object checkPropertyValue(SFSessionProperty property,
                                   Object propertyValue)
      throws SFException
  {
    if (propertyValue == null)
    {
      return null;
    }

    if (property.getValueType().isAssignableFrom(propertyValue.getClass()))
    {
      switch (property)
      {
        case APPLICATION:
          if (APPLICATION_REGEX.matcher((String)propertyValue).find())
          {
            return propertyValue;
          }
          else
          {
            throw new SFException(ErrorCode.INVALID_PARAMETER_VALUE,
                propertyValue, property);
          }
        default:
          return propertyValue;
      }
    }
    else
    {
      if (property.getValueType() == Boolean.class &&
          propertyValue instanceof String)
      {
        if ("on".equalsIgnoreCase((String) propertyValue) ||
            "true".equalsIgnoreCase((String) propertyValue))
        {
          return true;
        }
        else if ("off".equalsIgnoreCase((String) propertyValue) ||
            "false".equalsIgnoreCase((String) propertyValue))
        {
          return false;
        }
      }
      else if (property.getValueType() == Integer.class &&
          propertyValue instanceof String)
      {
        return Integer.valueOf((String)propertyValue);
      }
    }

    throw new SFException(ErrorCode.INVALID_PARAMETER_TYPE,
        propertyValue.getClass().getName(),
        property.getValueType().getName());
  }
}
