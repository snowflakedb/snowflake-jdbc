/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.util;

import java.util.Objects;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class SFTriple<F, S, T> {
  private final F first;
  private final S second;
  private final T third;

  public SFTriple(F first, S second, T third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public F first() {
    return first;
  }

  public S second() {
    return second;
  }

  public T third() {
    return third;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SFTriple<?, ?, ?> sfTriple = (SFTriple<?, ?, ?>) o;
    return Objects.equals(first, sfTriple.first)
        && Objects.equals(second, sfTriple.second)
        && Objects.equals(third, sfTriple.third);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third);
  }

  @Override
  public String toString() {
    return "[ " + first + ", " + second + ", " + third + " ]";
  }
}
