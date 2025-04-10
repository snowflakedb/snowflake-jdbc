package net.snowflake.client;

public class SystemPropertyOverrider {
  private final String propertyName;
  private final String oldValue;

  public SystemPropertyOverrider(String propertyName, String newValue) {
    this.propertyName = propertyName;
    this.oldValue = System.getProperty(propertyName);
    System.setProperty(propertyName, newValue);
  }

  public void rollback() {
    if (oldValue != null) {
      System.setProperty(propertyName, oldValue);
    } else {
      System.clearProperty(propertyName);
    }
  }
}
