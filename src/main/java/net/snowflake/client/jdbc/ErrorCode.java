/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.SqlState;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal JDBC driver error codes
 *
 * @author jhuang
 */
public enum ErrorCode
{

  /**
   * Error codes partitioning:
   *
   * 0NNNNN: GS SQL error codes
   * 1NNNNN: XP error codes
   * 2NNNNN: JDBC driver error codes
   * 3NNNNN: GS generic error codes
   * 4NNNNN: Node.js driver error codes
   *
   * N can be any digits from 0 to 9.
   */
  INTERNAL_ERROR(200001, SqlState.INTERNAL_ERROR),
  CONNECTION_ERROR(200002, SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
  INTERRUPTED(200003, SqlState.QUERY_CANCELED),
  COMPRESSION_TYPE_NOT_SUPPORTED(200004, SqlState.FEATURE_NOT_SUPPORTED),
  QUERY_CANCELED(200005, SqlState.QUERY_CANCELED),
  COMPRESSION_TYPE_NOT_KNOWN(200006, SqlState.FEATURE_NOT_SUPPORTED),
  FAIL_LIST_FILES(200007, SqlState.DATA_EXCEPTION),
  FILE_NOT_FOUND(200008, SqlState.DATA_EXCEPTION),
  FILE_IS_DIRECTORY(200009, SqlState.DATA_EXCEPTION),
  DUPLICATE_CONNECTION_PROPERTY_SPECIFIED(200010,
      SqlState.DATA_EXCEPTION),
  MISSING_USERNAME(200011, SqlState.INVALID_AUTHORIZATION_SPECIFICATION),
  MISSING_PASSWORD(200012, SqlState.INVALID_AUTHORIZATION_SPECIFICATION),
  S3_OPERATION_ERROR(200013, SqlState.SYSTEM_ERROR),
  MAX_RESULT_LIMIT_EXCEEDED(200014, SqlState.PROGRAM_LIMIT_EXCEEDED),
  NETWORK_ERROR(200015, SqlState.IO_ERROR),
  IO_ERROR(200016, SqlState.IO_ERROR),
  PATH_NOT_DIRECTORY(200017, SqlState.DATA_EXCEPTION),
  DATA_TYPE_NOT_SUPPORTED(200018, SqlState.FEATURE_NOT_SUPPORTED),
  CLIENT_SIDE_SORTING_NOT_SUPPORTED(200019, SqlState.FEATURE_NOT_SUPPORTED),
  AWS_CLIENT_ERROR(200020, SqlState.SYSTEM_ERROR),
  INVALID_SQL(200021, SqlState.SQL_STATEMENT_NOT_YET_COMPLETE),
  BAD_RESPONSE(200022, SqlState.INTERNAL_ERROR),
  ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED(200023, SqlState.FEATURE_NOT_SUPPORTED),
  STATEMENT_CLOSED(200024, SqlState.FEATURE_NOT_SUPPORTED),
  STATEMENT_ALREADY_RUNNING_QUERY(200025, SqlState.FEATURE_NOT_SUPPORTED),
  MISSING_SERVER_URL(200026, SqlState.INVALID_AUTHORIZATION_SPECIFICATION),
  TOO_MANY_SESSION_PARAMETERS(200027, SqlState.FEATURE_NOT_SUPPORTED),
  MISSING_CONNECTION_PROPERTY(200028,
      SqlState.INVALID_AUTHORIZATION_SPECIFICATION),
  INVALID_CONNECTION_URL(200029, SqlState.INVALID_AUTHORIZATION_SPECIFICATION),
  DUPLICATE_STATEMENT_PARAMETER_SPECIFIED(200030, SqlState.DATA_EXCEPTION),
  TOO_MANY_STATEMENT_PARAMETERS(200031, SqlState.FEATURE_NOT_SUPPORTED),
  COLUMN_DOES_NOT_EXIST(200032, SqlState.DATA_EXCEPTION),
  INVALID_PARAMETER_TYPE(200033, SqlState.INVALID_PARAMETER_VALUE),
  ROW_DOES_NOT_EXIST(200034, SqlState.DATA_EXCEPTION),
  FEATURE_UNSUPPORTED(200035, SqlState.FEATURE_NOT_SUPPORTED),
  INVALID_STATE(200036, SqlState.FEATURE_NOT_SUPPORTED),
  RESULTSET_ALREADY_CLOSED(200037, SqlState.FEATURE_NOT_SUPPORTED),
  INVALID_VALUE_CONVERT(200038, SqlState.FEATURE_NOT_SUPPORTED),
  IDP_CONNECTION_ERROR(200039,
                       SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
  IDP_INCORRECT_DESTINATION(200040,
                       SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
  ;

  public final static String errorMessageResource =
  "net.snowflake.client.jdbc.jdbc_error_messages";

  /**
   * Snowflake internal message associated to the error.
   */
  final private Integer messageCode;

  final private String sqlState;

  /**
   * Construct a new error code specification given Snowflake internal error
   * code and SQL state error code.
   * <p/>
   * @param messageCode
   */
  private ErrorCode(Integer messageCode, String sqlState)
  {
    this.messageCode = messageCode;
    this.sqlState = sqlState;
  }

  public Integer getMessageCode()
  {
    return messageCode;
  }

  public String getSqlState()
  {
    return sqlState;
  }

  @Override
  public String toString()
  {
    return "ErrorCode{" + "messageCode=" + messageCode + ", sqlState=" +
        sqlState + '}';
  }

  private static Map<Integer, ErrorCode> errorCodeMap = new HashMap<Integer, ErrorCode>();

  static
  {
    for (ErrorCode errorCode : ErrorCode.values())
    {
      errorCodeMap.put(errorCode.getMessageCode(), errorCode);
    }
  }

  public static ErrorCode getByErrorCode(String errorCode)
  {
    return errorCodeMap.get(errorCode);
  }
}