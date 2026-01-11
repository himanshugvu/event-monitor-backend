package com.vibe.events;

import com.vibe.events.config.AggregationProperties;
import com.vibe.events.registry.EventRegistryProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAsync
@EnableScheduling
@EnableCaching
@EnableConfigurationProperties({
  AggregationProperties.class,
  EventRegistryProperties.class
})
public class EventsApplication {
  public static void main(String[] args) {
    SpringApplication.run(EventsApplication.class, args);
  }
}
