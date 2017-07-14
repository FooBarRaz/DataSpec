package com.dataspec.exception;

public class ExpectationNeverMetException extends RuntimeException {

    public ExpectationNeverMetException(String message) {
        super(message);
    }
}
