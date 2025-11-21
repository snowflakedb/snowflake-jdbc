package net.snowflake.client.api.loader;

/**
 * Operations supported by the Snowflake Loader API.
 *
 * <p>These operations define how data is loaded into the target table. The operation type is
 * specified via the {@link LoaderProperty#operation} property.
 *
 * @see Loader
 * @see LoaderFactory
 * @see LoaderProperty#operation
 */
public enum Operation {
  /** Insert new rows into the target table */
  INSERT,

  /** Delete rows from the target table based on keys */
  DELETE,

  /** Modify existing rows in the target table */
  MODIFY,

  /** Insert new rows or update existing rows (update-or-insert) */
  UPSERT
}
