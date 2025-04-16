package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class QueryContextEntryDTOTest {

  @Mock private OpaqueContextDTO mockContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testDefaultConstructor() {
    QueryContextEntryDTO entry = new QueryContextEntryDTO();
    assertNotNull(entry);
    assertEquals(0, entry.getId());
    assertEquals(0, entry.getTimestamp());
    assertEquals(0, entry.getPriority());
    assertNull(entry.getContext());
  }

  @Test
  void testParameterizedConstructor() {
    QueryContextEntryDTO entry = new QueryContextEntryDTO(1L, 100L, 10L, mockContext);

    assertEquals(1L, entry.getId());
    assertEquals(100L, entry.getTimestamp());
    assertEquals(10L, entry.getPriority());
    assertEquals(mockContext, entry.getContext());
  }

  @Test
  void testSettersAndGetters() {
    QueryContextEntryDTO entry = new QueryContextEntryDTO();

    entry.setId(2L);
    entry.setTimestamp(200L);
    entry.setPriority(20L);
    entry.setContext(mockContext);

    assertEquals(2L, entry.getId());
    assertEquals(200L, entry.getTimestamp());
    assertEquals(20L, entry.getPriority());
    assertEquals(mockContext, entry.getContext());
  }
}
