package com.vibe.events.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.vibe.events.error.BadRequestException;
import java.time.LocalDate;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class DayValidatorTest {

  @Test
  void parseDayAcceptsValidDate() {
    LocalDate result = DayValidator.parseDay("2026-01-15");
    assertThat(result).isEqualTo(LocalDate.of(2026, 1, 15));
  }

  @Test
  void parseDayRejectsInvalidFormat() {
    assertThatThrownBy(() -> DayValidator.parseDay("2026/01/15"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("YYYY-MM-DD");
  }

  @Test
  void parseDayRejectsInvalidValue() {
    assertThatThrownBy(() -> DayValidator.parseDay("2026-13-40"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Invalid day value");
  }

  @Test
  void parseTimeAcceptsValidTime() {
    LocalTime result = DayValidator.parseTime("14:30");
    assertThat(result).isEqualTo(LocalTime.of(14, 30));
  }

  @Test
  void parseTimeRejectsInvalidFormat() {
    assertThatThrownBy(() -> DayValidator.parseTime("1430"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("HH:mm");
  }
}
