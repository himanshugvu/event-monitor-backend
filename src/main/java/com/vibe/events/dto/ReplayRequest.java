package com.vibe.events.dto;

import java.util.List;

public record ReplayRequest(String mode, String eventKey, String day, Long id, List<Long> ids) {}
