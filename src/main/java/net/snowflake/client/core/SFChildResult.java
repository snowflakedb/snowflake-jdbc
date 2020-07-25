package net.snowflake.client.core;

/** Data class to wrap information about child job results */
public class SFChildResult {
  // query id of child query, to look up child result
  String id;
  // statement type of child query, to properly interpret result
  SFStatementType type;

  public SFChildResult(String id, SFStatementType type) {
    this.id = id;
    this.type = type;
  }

  String getId() {
    return id;
  }

  SFStatementType getType() {
    return type;
  }
}
