/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/**
 * Created by jhuang on 11/3/15.
 */

/**
 * session properties accepted for opening a new session.
 */
public enum SFSessionProperty
{
  SERVER_URL("serverURL", true, String.class),
  USER("user", true, String.class),
  PASSWORD("password", true, String.class),
  ACCOUNT("account", true, String.class),
  DATABASE("database", false, String.class),
  SCHEMA("schema", false, String.class),
  PASSCODE_IN_PASSWORD("passcodeInPassword", false, Boolean.class),
  PASSCODE("passcode", false, String.class),
  TOKEN("token", false, String.class),
  ROLE("role", false, String.class),
  AUTHENTICATOR("authenticator", false, String.class),
  WAREHOUSE("warehouse", false, String.class),
  LOGIN_TIMEOUT("loginTimeout", false, Integer.class),
  NETWORK_TIMEOUT("networkTimeout", false, Integer.class),
  USE_PROXY("useProxy", false, Boolean.class),
  INJECT_SOCKET_TIMEOUT("injectSocketTimeout", false, Integer.class),
  INJECT_CLIENT_PAUSE("injectClientPause", false, Integer.class),
  APP_ID("appId", false, String.class),
  APP_VERSION("appVersion", false, String.class);

  private String propertyKey;
  private boolean required;
  private Class valueType;

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
                    Class valueType)
  {
    this.propertyKey = propertyKey;
    this.required = required;
    this.valueType = valueType;
  }

  static SFSessionProperty lookupByKey(String propertyKey)
  {
    for (SFSessionProperty property : SFSessionProperty.values())
    {
      if (property.propertyKey.equalsIgnoreCase(propertyKey))
        return property;
    }

    return  null;
  }
}
