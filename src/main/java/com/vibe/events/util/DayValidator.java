package com.vibe.events.util;

import com.vibe.events.error.BadRequestException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public final class DayValidator {
  private static final Pattern PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
  private static final Pattern TIME_PATTERN = Pattern.compile("^\\d{2}:\\d{2}(:\\d{2})?$");

  private DayValidator() {}

  public static LocalDate parseDay(String day) {
    if (day == null || !PATTERN.matcher(day).matches()) {
      throw new BadRequestException("Invalid day format, expected YYYY-MM-DD.");
    }
    try {
      return LocalDate.parse(day);
    } catch (DateTimeParseException ex) {
      throw new BadRequestException("Invalid day value.");
    }
  }

  public static LocalTime parseTime(String time) {
    if (time == null || time.isBlank()) {
      return null;
    }
    if (!TIME_PATTERN.matcher(time).matches()) {
      throw new BadRequestException("Invalid time format, expected HH:mm or HH:mm:ss.");
    }
    try {
      return LocalTime.parse(time);
    } catch (DateTimeParseException ex) {
      throw new BadRequestException("Invalid time value.");
    }
  }
}
