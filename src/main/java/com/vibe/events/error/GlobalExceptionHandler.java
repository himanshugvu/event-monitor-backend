package com.vibe.events.error;

import com.vibe.events.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {
  private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<ErrorResponse> handleNotFound(
      NotFoundException ex, HttpServletRequest request) {
    log.warn("Not found: {} {}", request.getMethod(), request.getRequestURI(), ex);
    return buildResponse(HttpStatus.NOT_FOUND, ex.getMessage(), request.getRequestURI());
  }

  @ExceptionHandler(BadRequestException.class)
  public ResponseEntity<ErrorResponse> handleBadRequest(
      BadRequestException ex, HttpServletRequest request) {
    log.warn("Bad request: {} {}", request.getMethod(), request.getRequestURI(), ex);
    return buildResponse(HttpStatus.BAD_REQUEST, ex.getMessage(), request.getRequestURI());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest request) {
    if (isClientAbort(ex)) {
      log.warn("Client aborted request: {} {}", request.getMethod(), request.getRequestURI());
      return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
    log.error("Unhandled error: {} {}", request.getMethod(), request.getRequestURI(), ex);
    return buildResponse(
        HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error", request.getRequestURI());
  }

  private boolean isClientAbort(Throwable error) {
    Throwable current = error;
    while (current != null) {
      String name = current.getClass().getName();
      if ("org.apache.catalina.connector.ClientAbortException".equals(name)
          || "org.springframework.web.context.request.async.AsyncRequestNotUsableException".equals(name)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private ResponseEntity<ErrorResponse> buildResponse(
      HttpStatus status, String message, String path) {
    ErrorResponse response =
        new ErrorResponse(LocalDateTime.now(), status.value(), status.getReasonPhrase(), message, path);
    return ResponseEntity.status(status).body(response);
  }
}
