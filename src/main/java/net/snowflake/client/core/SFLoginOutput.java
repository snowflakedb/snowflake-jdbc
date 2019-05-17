/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Map;

/**
 * Login output information including session tokens, database versions
 */
public class SFLoginOutput
{
  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;
  private String idToken;
  private String databaseVersion;
  private int databaseMajorVersion;
  private int databaseMinorVersion;
  private int httpClientSocketTimeout;
  private String sessionDatabase;
  private String sessionSchema;
  private String sessionRole;
  private String sessionWarehouse;
  private Map<String, Object> commonParams;
  private boolean updatedByTokenRequest;
  private boolean updatedByTokenRequestIssue;

  SFLoginOutput()
  {
  }

  SFLoginOutput(String sessionToken, String masterToken,
                long masterTokenValidityInSeconds,
                String idToken,
                String databaseVersion,
                int databaseMajorVersion, int databaseMinorVersion,
                int httpClientSocketTimeout,
                String sessionDatabase,
                String sessionSchema,
                String sessionRole,
                String sessionWarehouse,
                Map<String, Object> commonParams)
  {
    this.sessionToken = sessionToken;
    this.masterToken = masterToken;
    this.idToken = idToken;
    this.databaseVersion = databaseVersion;
    this.databaseMajorVersion = databaseMajorVersion;
    this.databaseMinorVersion = databaseMinorVersion;
    this.httpClientSocketTimeout = httpClientSocketTimeout;
    this.sessionDatabase = sessionDatabase;
    this.sessionSchema = sessionSchema;
    this.sessionRole = sessionRole;
    this.sessionWarehouse = sessionWarehouse;
    this.commonParams = commonParams;
    this.masterTokenValidityInSeconds = masterTokenValidityInSeconds;
  }

  public String getSessionToken()
  {
    return sessionToken;
  }

  public SFLoginOutput setSessionToken(String sessionToken)
  {
    this.sessionToken = sessionToken;
    return this;
  }

  String getMasterToken()
  {
    return masterToken;
  }

  SFLoginOutput setMasterToken(String masterToken)
  {
    this.masterToken = masterToken;
    return this;
  }

  String getIdToken()
  {
    return idToken;
  }

  SFLoginOutput setIdToken(String idToken)
  {
    this.idToken = idToken;
    return this;
  }

  String getDatabaseVersion()
  {
    return databaseVersion;
  }

  int getDatabaseMajorVersion()
  {
    return databaseMajorVersion;
  }

  int getDatabaseMinorVersion()
  {
    return databaseMinorVersion;
  }

  int getHttpClientSocketTimeout()
  {
    return httpClientSocketTimeout;
  }

  Map<String, Object> getCommonParams()
  {
    return commonParams;
  }

  SFLoginOutput setCommonParams(Map<String, Object> commonParams)
  {
    this.commonParams = commonParams;
    return this;
  }

  String getSessionDatabase()
  {
    return sessionDatabase;
  }

  void setSessionDatabase(String sessionDatabase)
  {
    this.sessionDatabase = sessionDatabase;
  }

  String getSessionSchema()
  {
    return sessionSchema;
  }

  void setSessionSchema(String sessionSchema)
  {
    this.sessionSchema = sessionSchema;
  }

  String getSessionRole()
  {
    return sessionRole;
  }

  void setSessionRole(String sessionRole)
  {
    this.sessionRole = sessionRole;
  }

  String getSessionWarehouse()
  {
    return sessionWarehouse;
  }

  void setSessionWarehouse(String sessionWarehouse)
  {
    this.sessionWarehouse = sessionWarehouse;
  }

  long getMasterTokenValidityInSeconds()
  {
    return masterTokenValidityInSeconds;
  }

  boolean isUpdatedByTokenRequest()
  {
    return updatedByTokenRequest;
  }

  SFLoginOutput setUpdatedByTokenRequest(boolean updatedByTokenRequest)
  {
    this.updatedByTokenRequest = updatedByTokenRequest;
    return this;
  }

  boolean isUpdatedByTokenRequestIssue()
  {
    return updatedByTokenRequestIssue;
  }

  SFLoginOutput setUpdatedByTokenRequestIssue(boolean updatedByTokenRequestIssue)
  {
    this.updatedByTokenRequestIssue = updatedByTokenRequestIssue;
    return this;
  }
}
