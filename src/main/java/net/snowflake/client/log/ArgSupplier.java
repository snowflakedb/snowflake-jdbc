/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

/**
 * An interface for representing lambda expressions that supply values to placeholders in message
 * formats.
 *
 * <p>E.g., {@code Logger.debug("Value: {}", (ArgSupplier) () -> getValue());}
 */
@FunctionalInterface
public interface ArgSupplier {
  Object get();
}
