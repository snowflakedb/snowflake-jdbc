/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.util.JsonStringHashMap;

class StructuredTypeConversionHelper {
  private static final Set<String> MAP_FIELDS =
      new HashSet<String>() {
        {
          add("key");
          add("value");
        }
      };

  static Object mapHashMapToObject(List<JsonStringHashMap<String, Object>> entriesList) {
    if (entriesList == null) {
      return null;
    }
    if (entriesList.stream().allMatch(it -> it.keySet().equals(MAP_FIELDS))) {
      // MAP is represented as list of entries with key and value
      return entriesList.stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.get("key"),
                  entry -> {
                    Object value = entry.get("value");
                    if (value instanceof List) {
                      List<?> list = (List<?>) value;
                      return mapListToObject(list);
                    }
                    return value;
                  }));
    } else {
      return entriesList;
    }
  }

  static Object mapListToObject(List<?> list) {
    if (list == null) {
      return null;
    }
    if (list.stream().anyMatch(nested -> nested instanceof JsonStringHashMap)) {
      return mapHashMapToObject((List<JsonStringHashMap<String, Object>>) list);
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

  static Object mapStructToObject(Map<?, ?> object) {
    if (object == null) {
      return null;
    }
    return object.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  Object value = e.getValue();
                  if (value instanceof List) {
                    return StructuredTypeConversionHelper.mapListToObject((List<?>) value);
                  } else if (value instanceof Map) {
                    return mapStructToObject((Map<?, ?>) value);
                  } else {
                    return value;
                  }
                }));
  }

  static String mapJson(Object ob) throws SFException {
    try {
      return SnowflakeUtil.mapJson(ob);
    } catch (JsonProcessingException e) {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, e.getMessage());
    }
  }
}
