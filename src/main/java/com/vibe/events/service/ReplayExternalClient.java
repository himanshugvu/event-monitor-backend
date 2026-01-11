package com.vibe.events.service;

import com.vibe.events.dto.ReplayExternalRequest;
import com.vibe.events.dto.ReplayExternalResponse;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class ReplayExternalClient {
  private final RestClient restClient;

  public ReplayExternalClient() {
    this.restClient = RestClient.builder().build();
  }

  public ReplayExternalResponse replay(String replayUrl, ReplayExternalRequest request) {
    ReplayExternalRequest resolvedRequest =
        request == null
            ? new ReplayExternalRequest(List.of(), UUID.randomUUID().toString(), null, null, null)
            : request;
    return restClient
        .post()
        .uri(replayUrl)
        .body(resolvedRequest)
        .retrieve()
        .body(ReplayExternalResponse.class);
  }
}
