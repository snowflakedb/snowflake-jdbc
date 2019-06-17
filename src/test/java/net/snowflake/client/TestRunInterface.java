/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import net.snowflake.client.core.SFException;

/**
 * Functional interface used to run a piece of code which throws SFException
 */
@FunctionalInterface
public interface TestRunInterface
{
  void run() throws SFException;
}
