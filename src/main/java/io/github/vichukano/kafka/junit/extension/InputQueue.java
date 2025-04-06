package io.github.vichukano.kafka.junit.extension;

import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface InputQueue {

    Class<?> keySerializer() default StringSerializer.class;

    Class<?> valueSerializer() default StringSerializer.class;

    String bootstrapServers() default "${spring.embedded.kafka.brokers}";

    String[] additionalProperties() default {};

}
