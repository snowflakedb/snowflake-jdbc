package net.snowflake.client.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SFPairTest {

  @Test
  void testOfMethod() {
    SFPair<String, Integer> pair = SFPair.of("Test", 123);
    assertNotNull(pair);
    assertEquals("Test", pair.left);
    assertEquals(123, pair.right);
  }

  @Test
  void testEqualsMethod() {
    SFPair<String, Integer> pair1 = SFPair.of("Test", 123);
    SFPair<String, Integer> pair2 = SFPair.of("Test", 123);
    SFPair<String, Integer> pair3 = SFPair.of("Different", 123);
    SFPair<String, Integer> pair4 = SFPair.of("Test", 999);
    SFPair<String, Integer> pair5 = SFPair.of("Different", 999);

    assertEquals(pair1, pair2);
    assertNotEquals(pair1, pair3);
    assertNotEquals(pair1, pair4);
    assertNotEquals(pair1, pair5);
    assertNotEquals(pair1, null);
    assertNotEquals(pair1, "Not an SFPair");
    assertEquals(pair1, pair1);
  }

  @Test
  void testHashCodeMethod() {
    SFPair<String, Integer> pair1 = SFPair.of("Test", 123);
    SFPair<String, Integer> pair2 = SFPair.of("Test", 123);
    SFPair<String, Integer> pair3 = SFPair.of("Test", 999);

    assertEquals(pair1.hashCode(), pair2.hashCode());
    assertNotEquals(pair1.hashCode(), pair3.hashCode());
  }

  @Test
  void testToStringMethod() {
    SFPair<String, Integer> pair = SFPair.of("Test", 123);
    assertEquals("[ Test, 123 ]", pair.toString());

    SFPair<String, String> nullPair = SFPair.of(null, null);
    assertEquals("[ null, null ]", nullPair.toString());
  }
}
