package net.snowflake.client.core;

class HostSpecSSD {
  private String hostname;
  private String hostSpecDirective;

  HostSpecSSD() {
    this.hostname = null;
    this.hostSpecDirective = null;
  }

  String getHostname() {
    return this.hostname;
  }

  String getHostSpecDirective() {
    return this.hostSpecDirective;
  }

  void setHostname(String hname) {
    this.hostname = hname;
  }

  void setHostSpecDirective(String ssd) {
    this.hostSpecDirective = ssd;
  }
}
