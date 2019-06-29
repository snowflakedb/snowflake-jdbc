/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Abstract class of arrow vector converter. For most types, throw invalid
 * convert error. It depends child class to override conversion logic
 * <p>
 * Note: two method toObject and toString is abstract method because every
 * converter implementation needs to implement them
 */
abstract class AbstractArrowVectorConverter implements ArrowVectorConverter
{
  /**
   * snowflake logical type of the target arrow vector
   */
  String logicalTypeStr;

  /**
   * value vector
   */
  private ValueVector valueVector;

  protected DataConversionContext context;

  AbstractArrowVectorConverter(String logicalTypeStr,
                               ValueVector valueVector,
                               DataConversionContext context)
  {
    this.logicalTypeStr = logicalTypeStr;
    this.valueVector = valueVector;
    this.context = context;
  }

  @Override
  public boolean toBoolean(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "boolean",
                          "");
  }

  @Override
  public byte toByte(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "byte",
                          "");
  }

  @Override
  public short toShort(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "short",
                          "");
  }

  @Override
  public int toInt(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "int");
  }

  @Override
  public long toLong(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "long",
                          "");
  }

  @Override
  public double toDouble(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "double",
                          "");
  }

  @Override
  public float toFloat(int rowIndex) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "float",
                          "");
  }

  @Override
  public byte[] toBytes(int index) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "byteArray",
                          "");
  }

  @Override
  public Date toDate(int index) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "Date",
                          "");
  }

  @Override
  public Time toTime(int index) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "Time",
                          "");
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "Timestamp",
                          "");
  }

  @Override
  public BigDecimal toBigDecimal(int index) throws SFException
  {
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                          logicalTypeStr,
                          "BigDecimal",
                          "");
  }

  @Override
  public boolean isNull(int index)
  {
    return valueVector.isNull(index);
  }

  @Override
  abstract public Object toObject(int index) throws SFException;

  @Override
  abstract public String toString(int index) throws SFException;
}
