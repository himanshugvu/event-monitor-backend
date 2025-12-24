package com.vibe.events.controller;

import com.vibe.events.dto.EventCatalogItem;
import com.vibe.events.registry.EventDefinition;
import com.vibe.events.registry.EventRegistry;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class EventCatalogController {
  private final EventRegistry registry;

  public EventCatalogController(EventRegistry registry) {
    this.registry = registry;
  }

  @GetMapping
  public List<EventCatalogItem> listEvents() {
    return registry.all().stream()
        .map(EventCatalogController::toItem)
        .toList();
  }

  private static EventCatalogItem toItem(EventDefinition definition) {
    String name = definition.getName();
    if (name == null || name.isBlank()) {
      name = definition.getKey();
    }
    String category = definition.getCategory();
    if (category == null || category.isBlank()) {
      category = "Uncategorized";
    }
    return new EventCatalogItem(definition.getKey(), name, category);
  }
}
