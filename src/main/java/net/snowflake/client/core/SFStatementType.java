/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.snowflake.client.core;

/**
 * Used to check if the statementType belongs to DDL or DML The enum of each statement type is
 * defined in com.snowflake.core.Statement.java
 */
public enum SFStatementType {
  /**
   * By default we set query will generate result set, which means executeUpdate will throw
   * exception. In that way, we have a clear control of what statements can be executed by
   * executeUpdate
   */
  UNKNOWN(0x0000, true),

  SELECT(0x1000, true),

  /** Data Manipulation Language */
  DML(0x3000, false),
  INSERT(0x3000 + 0x100, false),
  UPDATE(0x3000 + 0x200, false),
  DELETE(0x3000 + 0x300, false),
  MERGE(0x3000 + 0x400, false),
  MULTI_INSERT(0x3000 + 0x500, false),
  COPY(0x3000 + 0x600, false),
  UNLOAD(0x3000 + 0x700, false),
  RECLUSTER(0x3000 + 0x800, false),

  /** System Command Language (USE, DESCRIBE etc) */
  SCL(0x4000, false),
  ALTER_SESSION(0x4000 + 0x100, false),
  USE(0x4000 + 0x300, false),
  USE_DATABASE(0x4000 + 0x300 + 0x01, false),
  USE_SCHEMA(0x4000 + 0x300 + 0x02, false),
  USE_WAREHOUSE(0x4000 + 0x300 + 0x03, false),
  SHOW(0x4000 + 0x400, true),
  DESCRIBE(0x4000 + 0x500, true),
  LIST(0x4000 + 0x700 + 0x01, true),

  /** Transaction Command Language (COMMIT, ROLLBACK) */
  TCL(0x5000, false),

  /** Data Definition Language */
  DDL(0x6000, false),

  /** Stage-related commands (other than LIST) */
  GET(0x7000 + 0x100 + 0x01, true),
  PUT(0x7000 + 0x100 + 0x02, true),
  REMOVE(0x7000 + 0x100 + 0x03, true),
  ;

  private final long statementTypeId;

  /**
   * Used by Statement.executeUpdate to determine if we should return update counts or throw
   * exception. In general, JDBC should only throw exception if a result set object is required.
   */
  private final boolean generateResultSet;

  private static final long LEVEL_3_RANGE = 0x1000;

  SFStatementType(long id, boolean generateResultSet) {
    this.statementTypeId = id;
    this.generateResultSet = generateResultSet;
  }

  public static SFStatementType lookUpTypeById(long id) {
    for (SFStatementType type : SFStatementType.values()) {
      if (type.getStatementTypeId() == id) {
        return type;
      }
    }

    // if not specific type is found, then return category of statement
    if (id >= SCL.getStatementTypeId() && id < SCL.getStatementTypeId() + LEVEL_3_RANGE) {
      return SCL;
    } else if (id >= TCL.getStatementTypeId() && id < TCL.getStatementTypeId() + LEVEL_3_RANGE) {
      return TCL;
    } else if (id >= DDL.getStatementTypeId() && id < DDL.getStatementTypeId() + LEVEL_3_RANGE) {
      return DDL;
    } else {
      return UNKNOWN;
    }
  }

  public long getStatementTypeId() {
    return statementTypeId;
  }

  public boolean isDDL() {
    return this == DDL;
  }

  public boolean isDML() {
    return statementTypeId >= DML.getStatementTypeId()
        && statementTypeId < DML.getStatementTypeId() + LEVEL_3_RANGE;
  }

  public boolean isTCL() {
    return this == TCL;
  }

  public boolean isSCL() {
    return this == SCL;
  }

  public boolean isGenerateResultSet() {
    return this.generateResultSet;
  }

  public boolean isSelect() {
    return this.statementTypeId == SELECT.statementTypeId;
  }
}
