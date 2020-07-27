package net.snowflake.client.core;

class KeyUpdSSD {
  private String issuer;
  private String keyUpdDirective;

  KeyUpdSSD() {
    this.issuer = null;
    this.keyUpdDirective = null;
  }

  String getIssuer() {
    return this.issuer;
  }

  String getKeyUpdDirective() {
    return this.keyUpdDirective;
  }

  void setIssuer(String issuer) {
    this.issuer = issuer;
  }

  void setKeyUpdDirective(String ssd) {
    this.keyUpdDirective = ssd;
  }
}
