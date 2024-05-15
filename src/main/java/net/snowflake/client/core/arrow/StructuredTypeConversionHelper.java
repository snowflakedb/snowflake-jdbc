/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import org.apache.arrow.vector.util.JsonStringHashMap;

class StructuredTypeConversionHelper {
  private static final Set<String> MAP_FIELDS =
      new HashSet<String>() {
        {
          add("key");
          add("value");
        }
      };

  static Object mapHashMapToObject(List<JsonStringHashMap<String, Object>> entriesList)
      throws SFException {
    if (entriesList.stream().allMatch(it -> it.keySet().equals(MAP_FIELDS))) {
        // MAP is represented as list of entries with key and value
      return entriesList.stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.get("key").toString(),
                  entry -> {
                    Object value = entry.get("value");
                    if (value instanceof List) {
                      List<?> list = (List) value;
                      return mapListToObject(list);
                    }
                    return value;
                  }));
    } else {
        return entriesList;
    }
  }

  static Object mapListToObject(List<?> list) {
    if (list.stream().anyMatch(nested -> nested instanceof JsonStringHashMap)) {
      try {
        return mapHashMapToObject((List<JsonStringHashMap<String, Object>>) list);
      } catch (SFException e) {
        throw new RuntimeException(e);
      }
    } else {
      return list.stream()
          .map(
              nested -> {
                if (nested instanceof List) {
                  return mapListToObject((List<?>) nested);
                } else {
                  return nested;
                }
              })
          .collect(Collectors.toList());
    }
  }
}
