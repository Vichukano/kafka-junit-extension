package io.github.kafka.junit.extension;

public class TestConsumerConditionException extends RuntimeException {

    public TestConsumerConditionException(String message) {
        super(message);
    }

    public TestConsumerConditionException(String message, Throwable cause) {
        super(message, cause);
    }

}
