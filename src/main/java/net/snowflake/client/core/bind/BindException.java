/*
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.bind;

import net.snowflake.client.jdbc.telemetry.TelemetryField;

public class BindException extends Exception
{
  public enum Type
  {
    SERIALIZATION(TelemetryField.FAILED_BIND_SERIALIZATION),
    UPLOAD(TelemetryField.FAILED_BIND_UPLOAD),
    OTHER(TelemetryField.FAILED_BIND_OTHER)
    ;

    public final TelemetryField field;
    Type(TelemetryField field)
    {
      this.field = field;
    }
  }

  public final Type type;

  public BindException(String msg, Type type)
  {
    super(msg);
    this.type = type;
  }
}
