package com.vibe.events.util;

import com.vibe.events.error.BadRequestException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public final class DayValidator {
  private static final Pattern PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

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
}
