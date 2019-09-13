/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

/**
 * Telemetry client that is doing nothing. Mainly used in testing code
 */
public class NoOpTelemetryClient implements Telemetry
{
  @Override
  public void tryAddLogToBatch(TelemetryData log)
  {
  }

  @Override
  public void addLogToBatch(TelemetryData log)
  {
  }

  @Override
  public void close()
  {
  }

  @Override
  public boolean sendBatch()
  {
    return false;
  }
}
