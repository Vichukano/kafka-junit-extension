package io.github.kafka.junit.extension;

import org.apache.kafka.common.serialization.StringDeserializer;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface TestConsumer {
    String topic();

    Class<?> keyDeserializer() default StringDeserializer.class;

    Class<?> valueDeserializer() default StringDeserializer.class;

    int partitions() default 2;

    String bootstrapServers() default "${spring.embedded.kafka.brokers}";

    String[] additionalProperties() default {};
}
