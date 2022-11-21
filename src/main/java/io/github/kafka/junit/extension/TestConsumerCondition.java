package io.github.kafka.junit.extension;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class TestConsumerCondition implements ExecutionCondition, BeforeTestExecutionCallback, AfterAllCallback {
    private final Map<String, TestKafkaConsumer> testConsumers = new ConcurrentHashMap<>();

    @SneakyThrows
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return ConditionEvaluationResult.enabled("Test consumer enabled");
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        final Optional<Object> testObjOpt = context.getTestInstance();
        log.trace("Test object: {}", testObjOpt);
        testObjOpt.ifPresent(this::processTestInstance);
        log.trace("Finish processing for: {}", testObjOpt);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        testConsumers.forEach((k, v) -> {
            if (v != null) {
                v.stop();
            }
        });
    }

    private void processTestInstance(Object testInstance) {
        Arrays.stream(testInstance.getClass().getDeclaredFields())
            .filter(f -> Arrays.stream(f.getAnnotations()).anyMatch(a -> a instanceof TestConsumer))
            .forEach(f -> processAnnotatedField(f, testInstance));
    }

    private void processAnnotatedField(Field field, Object testInstance) {
        try {
            log.trace("Start to process field: {} of object: {}", field, testInstance);
            if (!field.getType().isAssignableFrom(BlockingQueue.class)) {
                throw new TestConsumerConditionException("Wrong class, should be: " + BlockingQueue.class);
            }
            if (testConsumers.get(field.getName()) != null) {
                log.debug("Test consumer already configured");
                field.setAccessible(true);
                field.set(testInstance, testConsumers.get(field.getName()).getRecordsQueue());
            } else {
                final TestConsumer annotation = field.getAnnotation(TestConsumer.class);
                TestKafkaConsumer tk = consumer(annotation);
                tk.start();
                testConsumers.put(field.getName(), tk);
                final BlockingQueue<ConsumerRecord<Object, Object>> recordsQueue = tk.getRecordsQueue();
                field.setAccessible(true);
                field.set(testInstance, recordsQueue);
                log.trace("Finish processing for object: {}, field: {}", testInstance, field);
            }
        } catch (Exception e) {
            throw new TestConsumerConditionException("Failed to process annotated field", e);
        }
    }

    private TestKafkaConsumer consumer(TestConsumer annotation) {
        final String[] additionalProperties = annotation.additionalProperties();
        final String bootstrapServers = annotation.bootstrapServers();
        final String servers;
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("BootstrapServers is absent");
        }
        if (bootstrapServers.startsWith("${") && bootstrapServers.endsWith("}")) {
            servers = System.getProperty(bootstrapServers.replaceAll("[\\$, \\{,\\}]", ""));
            if (servers == null) {
                throw new IllegalArgumentException("Failed to resolve bootstrap servers from: " + bootstrapServers);
            }
        } else {
            servers = bootstrapServers;
        }
        final int partitions = annotation.partitions();
        final String topic = annotation.topic();
        final Class<?> keyDes = annotation.keyDeserializer();
        final Class<?> valDes = annotation.valueDeserializer();
        TestKafkaConsumer testKafkaConsumer;
        if (additionalProperties.length == 0) {
            testKafkaConsumer = TestKafkaConsumer.consumer(
                topic,
                servers,
                partitions,
                keyDes,
                valDes
            );
        } else {
            testKafkaConsumer = TestKafkaConsumer.consumerWithAdditionalProperties(
                topic,
                servers,
                partitions,
                keyDes,
                valDes,
                Arrays.stream(additionalProperties).map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> s[1]))
            );
        }
        return testKafkaConsumer;
    }
}
