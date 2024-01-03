package net.snowflake.client.core;

/** Data class to wrap information about child job results */
public class SFChildResult {
  // query id of child query, to look up child result
  private final String id;
  // statement type of child query, to properly interpret result
  private final SFStatementType type;

  public SFChildResult(String id, SFStatementType type) {
    this.id = id;
    this.type = type;
  }

  // For Snowflake internal use
  public String getId() {
    return id;
  }

  // For Snowflake internal use
  public SFStatementType getType() {
    return type;
  }
}
