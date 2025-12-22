package com.vibe.events.registry;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "events")
public class EventRegistryProperties {
  private List<EventDefinition> registry = new ArrayList<>();

  public List<EventDefinition> getRegistry() {
    return registry;
  }

  public void setRegistry(List<EventDefinition> registry) {
    this.registry = registry;
  }
}
