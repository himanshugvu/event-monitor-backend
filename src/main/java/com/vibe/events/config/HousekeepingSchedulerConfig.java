package com.vibe.events.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class HousekeepingSchedulerConfig {
  @Bean
  public TaskScheduler housekeepingTaskScheduler(HousekeepingProperties properties) {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(Math.max(2, properties.getSchedulerPoolSize()));
    scheduler.setThreadNamePrefix("housekeeping-");
    scheduler.initialize();
    return scheduler;
  }
}
