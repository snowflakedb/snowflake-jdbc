package net.snowflake.client.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.common.core.SqlState;

/** Statement metadata which includes the result metadata and bind information. */
public class SFPreparedStatementMetaData {
  // result metadata
  private SFResultSetMetaData resultSetMetaData;

  // number of binds
  private int numberOfBinds;

  private final SFStatementType statementType;

  private final boolean arrayBindSupported;

  private List<MetaDataOfBinds> metaDataOfBinds;

  private final boolean isValidMetaData;

  public SFPreparedStatementMetaData(
      SFResultSetMetaData resultSetMetaData,
      SFStatementType statementType,
      int numberOfBinds,
      boolean arrayBindSupported,
      List<MetaDataOfBinds> metaDataOfBinds,
      boolean isValidMetaData) {
    this.resultSetMetaData = resultSetMetaData;
    this.statementType = statementType;
    this.numberOfBinds = numberOfBinds;
    this.arrayBindSupported = arrayBindSupported;
    this.metaDataOfBinds = metaDataOfBinds;
    this.isValidMetaData = isValidMetaData;
  }

  public SFResultSetMetaData getResultSetMetaData() {
    return resultSetMetaData;
  }

  public void setResultSetMetaData(SFResultSetMetaData resultSetMetaData) {
    this.resultSetMetaData = resultSetMetaData;
  }

  public int getNumberOfBinds() {
    return numberOfBinds;
  }

  public MetaDataOfBinds getMetaDataForBindParam(int param) throws SQLException {
    if (param < 1 || param > numberOfBinds) {
      throw new SnowflakeSQLException(
          SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
          ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE.getMessageCode(),
          param,
          numberOfBinds);
    }
    if (numberOfBinds != metaDataOfBinds.size() || metaDataOfBinds.size() == 0) {
      throw new SnowflakeSQLException(SqlState.NO_DATA, ErrorCode.NO_VALID_DATA.getMessageCode());
    }
    return metaDataOfBinds.get(param - 1);
  }

  public void setNumberOfBinds(int numberOfBinds) {
    this.numberOfBinds = numberOfBinds;
  }

  /**
   * Is a valid metadata or not. If true, this object is a valid metadata from describe. If false, a
   * dummy/empty metadata generated because prepare statement fails.
   *
   * <p>This is used to determine if the content is valid or not, e.g., number of bind parameters.
   *
   * @return true or false
   */
  public boolean isValidMetaData() {
    return isValidMetaData;
  }

  /**
   * According to StatementType, to decide whether array binds supported or not
   *
   * <p>Currently, only INSERT supports array bind
   *
   * @return true if array binds is supported.
   */
  public boolean isArrayBindSupported() {
    return this.arrayBindSupported;
  }

  public SFStatementType getStatementType() {
    return this.statementType;
  }

  /**
   * Generates an empty/invalid metadata for placeholder.
   *
   * @return statement metadata
   */
  public static SFPreparedStatementMetaData emptyMetaData() {
    return new SFPreparedStatementMetaData(
        new SFResultSetMetaData(
            0,
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<Integer>emptyList(),
            null),
        SFStatementType.UNKNOWN,
        0,
        false,
        new ArrayList<>(),
        false); // invalid metadata
  }
}
