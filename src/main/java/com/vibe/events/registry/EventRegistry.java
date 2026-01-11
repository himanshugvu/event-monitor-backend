package com.vibe.events.registry;

import com.vibe.events.error.NotFoundException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class EventRegistry {
  private final Map<String, EventDefinition> byKey;

  public EventRegistry(EventRegistryProperties properties) {
    Map<String, EventDefinition> map = new LinkedHashMap<>();
    for (EventDefinition definition : properties.getRegistry()) {
      if (definition.getKey() == null || definition.getKey().isBlank()) {
        continue;
      }
      if (definition.getSuccessTable() == null
          || definition.getSuccessTable().isBlank()
          || definition.getFailureTable() == null
          || definition.getFailureTable().isBlank()) {
        throw new IllegalStateException(
            "Event tables must be provided for key: " + definition.getKey());
      }
      if (definition.getReplayUrl() == null || definition.getReplayUrl().isBlank()) {
        throw new IllegalStateException(
            "Replay URL must be provided for key: " + definition.getKey());
      }
      if (map.containsKey(definition.getKey())) {
        throw new IllegalStateException("Duplicate event key: " + definition.getKey());
      }
      map.put(definition.getKey(), definition);
    }
    this.byKey = Collections.unmodifiableMap(map);
  }

  public List<EventDefinition> all() {
    return List.copyOf(byKey.values());
  }

  public EventDefinition getRequired(String eventKey) {
    EventDefinition definition = byKey.get(eventKey);
    if (definition == null) {
      throw new NotFoundException("Unknown eventKey: " + eventKey);
    }
    return definition;
  }

  public String successTable(String eventKey) {
    return getRequired(eventKey).getSuccessTable();
  }

  public String failureTable(String eventKey) {
    return getRequired(eventKey).getFailureTable();
  }

  public String replayUrl(String eventKey) {
    return getRequired(eventKey).getReplayUrl();
  }
}
