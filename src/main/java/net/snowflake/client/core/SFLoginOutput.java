/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Map;

/** Login output information including session tokens, database versions */
public class SFLoginOutput {
  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;
  private String idToken;
  private String mfaToken;
  private String databaseVersion;
  private int databaseMajorVersion;
  private int databaseMinorVersion;
  private int httpClientSocketTimeout;
  private String sessionDatabase;
  private String sessionSchema;
  private String sessionRole;
  private String sessionWarehouse;
  private Map<String, Object> commonParams;

  private String sessionId;

  SFLoginOutput() {}

  SFLoginOutput(
      String sessionToken,
      String masterToken,
      long masterTokenValidityInSeconds,
      String idToken,
      String mfaToken,
      String databaseVersion,
      int databaseMajorVersion,
      int databaseMinorVersion,
      int httpClientSocketTimeout,
      String sessionDatabase,
      String sessionSchema,
      String sessionRole,
      String sessionWarehouse,
      String sessionId,
      Map<String, Object> commonParams) {
    this.sessionToken = sessionToken;
    this.masterToken = masterToken;
    this.idToken = idToken;
    this.mfaToken = mfaToken;
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
    this.sessionId = sessionId;
  }

  public boolean getAutoCommit() {
    return (Boolean) this.commonParams.get("AUTOCOMMIT");
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public SFLoginOutput setSessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
    return this;
  }

  String getMasterToken() {
    return masterToken;
  }

  SFLoginOutput setMasterToken(String masterToken) {
    this.masterToken = masterToken;
    return this;
  }

  String getIdToken() {
    return idToken;
  }

  String getMfaToken() {
    return mfaToken;
  }

  String getDatabaseVersion() {
    return databaseVersion;
  }

  int getDatabaseMajorVersion() {
    return databaseMajorVersion;
  }

  int getDatabaseMinorVersion() {
    return databaseMinorVersion;
  }

  int getHttpClientSocketTimeout() {
    return httpClientSocketTimeout;
  }

  Map<String, Object> getCommonParams() {
    return commonParams;
  }

  String getSessionDatabase() {
    return sessionDatabase;
  }

  String getSessionSchema() {
    return sessionSchema;
  }

  String getSessionRole() {
    return sessionRole;
  }

  String getSessionWarehouse() {
    return sessionWarehouse;
  }

  long getMasterTokenValidityInSeconds() {
    return masterTokenValidityInSeconds;
  }
}
