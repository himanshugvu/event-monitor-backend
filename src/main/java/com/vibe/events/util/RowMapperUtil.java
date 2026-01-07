package com.vibe.events.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.springframework.jdbc.core.RowMapper;

public final class RowMapperUtil {
  private static final Set<String> STRING_COLUMNS =
      Set.of("id", "source_payload", "transformed_payload");

  private RowMapperUtil() {}

  public static RowMapper<Map<String, Object>> dynamicRowMapper() {
    return RowMapperUtil::mapRow;
  }

  private static Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int count = meta.getColumnCount();
    Map<String, Object> row = new LinkedHashMap<>(count);
    for (int i = 1; i <= count; i++) {
      String column = meta.getColumnLabel(i);
      Object value = rs.getObject(i);
      if (column == null) {
        row.put(null, value);
        continue;
      }
      String normalized = column.toLowerCase(Locale.ROOT);
      if (STRING_COLUMNS.contains(normalized)) {
        row.put(column, Objects.toString(value, null));
      } else {
        row.put(column, value);
      }
    }
    return row;
  }
}
