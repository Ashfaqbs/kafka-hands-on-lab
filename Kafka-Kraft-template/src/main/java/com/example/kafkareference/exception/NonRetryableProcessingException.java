package com.example.kafkareference.exception;

/**
 * Thrown when processing fails due to a permanent error (invalid data, business
 * rule violation). The DefaultErrorHandler sends this IMMEDIATELY to DLT without
 * retrying — no point retrying something that will always fail.
 *
 * Examples: missing required field, negative price, unknown currency code.
 */
public class NonRetryableProcessingException extends RuntimeException {

    public NonRetryableProcessingException(String message) {
        super(message);
    }

    public NonRetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
