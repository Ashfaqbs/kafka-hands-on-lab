package com.example.kafkareference.exception;

/**
 * Thrown when processing fails due to a transient error (DB timeout, downstream
 * service temporarily unavailable). The DefaultErrorHandler will retry this
 * up to max-attempts with exponential backoff before sending to DLT.
 */
public class RetryableProcessingException extends RuntimeException {

    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
