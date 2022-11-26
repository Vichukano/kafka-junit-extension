package io.github.vichukano.kafka.junit.extension;

public class KafkaQueuesException extends RuntimeException {

    public KafkaQueuesException(String message) {
        super(message);
    }

    public KafkaQueuesException(String message, Throwable cause) {
        super(message, cause);
    }

}
