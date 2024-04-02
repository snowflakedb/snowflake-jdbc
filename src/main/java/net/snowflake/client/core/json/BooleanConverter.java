package net.snowflake.client.core.json;

import java.sql.Types;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;

public class BooleanConverter {
  public Boolean getBoolean(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return false;
    }
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    }
    // if type is an approved type that can be converted to Boolean, do this
    if (columnType == Types.BOOLEAN
        || columnType == Types.INTEGER
        || columnType == Types.SMALLINT
        || columnType == Types.TINYINT
        || columnType == Types.BIGINT
        || columnType == Types.BIT
        || columnType == Types.VARCHAR
        || columnType == Types.CHAR
        || columnType == Types.DECIMAL) {
      String type = obj.toString();
      if ("1".equals(type) || Boolean.TRUE.toString().equalsIgnoreCase(type)) {
        return true;
      }
      if ("0".equals(type) || Boolean.FALSE.toString().equalsIgnoreCase(type)) {
        return false;
      }
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BOOLEAN_STR, obj);
  }
}
