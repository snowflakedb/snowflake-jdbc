package net.snowflake.client.core.arrow.tostringhelpers;

import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.jdbc.SnowflakeType;

public abstract class ArrowStringRepresentationBuilderBase {
  private final StringBuilder stringBuilder;
  private boolean isFirstValue = true;
  private static final Set<SnowflakeType> quotableTypes;

  static {
    quotableTypes = new HashSet<>();
    quotableTypes.add(SnowflakeType.INTEGER);
    quotableTypes.add(SnowflakeType.ANY);
    quotableTypes.add(SnowflakeType.CHAR);
    quotableTypes.add(SnowflakeType.TEXT);
    quotableTypes.add(SnowflakeType.VARIANT);
    quotableTypes.add(SnowflakeType.BINARY);
    quotableTypes.add(SnowflakeType.DATE);
    quotableTypes.add(SnowflakeType.TIME);
    quotableTypes.add(SnowflakeType.TIMESTAMP_LTZ);
    quotableTypes.add(SnowflakeType.TIMESTAMP_NTZ);
    quotableTypes.add(SnowflakeType.TIMESTAMP_TZ);
  }

  public ArrowStringRepresentationBuilderBase() {
    stringBuilder = new StringBuilder();
  }

  public ArrowStringRepresentationBuilderBase append(String string) {
    stringBuilder.append(string);
    return this;
  }

  public ArrowStringRepresentationBuilderBase append(char character) {
    stringBuilder.append(character);
    return this;
  }

  protected ArrowStringRepresentationBuilderBase appendQuoted(String value) {
    return this.append('"').append(value).append('"');
  }

  protected void addCommaIfNeeded() {
    if (!isFirstValue) {
      this.append(",");
    }
    this.isFirstValue = false;
  }

  protected boolean shouldQuoteValue(SnowflakeType snowflakeType) {
    return quotableTypes.contains(snowflakeType);
  }

  @Override
  public String toString() {
    return stringBuilder.toString();
  }
}
