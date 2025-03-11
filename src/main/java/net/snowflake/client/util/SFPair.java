package net.snowflake.client.util;

import java.util.Objects;

public class SFPair<L, R> {
  public L left;

  public R right;

  public static <L, R> SFPair<L, R> of(L l, R r) {
    return new SFPair<>(l, r);
  }

  private SFPair(L left, R right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (other == this) {
      return true;
    }

    if (!(SFPair.class.isInstance(other))) {
      return false;
    }

    SFPair<?, ?> pair2 = (SFPair<?, ?>) other;
    return Objects.equals(this.left, pair2.left) && Objects.equals(this.right, pair2.right);
  }

  @Override
  public int hashCode() {
    int result = 0;
    if (left != null) {
      result += 37 * left.hashCode();
    }
    if (right != null) {
      result += right.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "[ " + left + ", " + right + " ]";
  }
}
