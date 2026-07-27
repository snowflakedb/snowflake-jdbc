package net.snowflake.client.internal.log;

/**
 * An interface for representing lambda expressions that supply values to placeholders in message
 * formats.
 *
 * <p>E.g., {@code Logger.debug("Value: {}", (ArgSupplier) () -> getValue());}
 */
@FunctionalInterface
public interface ArgSupplier {
  /**
   * Get value
   *
   * @return Object value.
   */
  Object get();
}
