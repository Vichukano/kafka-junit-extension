package io.github.vichukano.kafka.junit.extension;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
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
public class KafkaQueuesCondition implements ExecutionCondition, BeforeTestExecutionCallback, AfterAllCallback {
    private final Map<String, KafkaOutputQueueProvider> testConsumers = new ConcurrentHashMap<>();
    private final Map<String, KafkaInputQueueProvider> testProducers = new ConcurrentHashMap<>();

    @SneakyThrows
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return ConditionEvaluationResult.enabled("Test Kafka queues enabled");
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
        testProducers.forEach((k, v) -> {
            if (v != null) {
                v.stop();
            }
        });
    }

    private void processTestInstance(Object testInstance) {
        Arrays.stream(testInstance.getClass().getDeclaredFields())
            .filter(f -> Arrays.stream(f.getAnnotations()).anyMatch(a -> a instanceof OutputQueue))
            .forEach(f -> processOutputQueueAnnotatedField(f, testInstance));
        Arrays.stream(testInstance.getClass().getDeclaredFields())
            .filter(f -> Arrays.stream(f.getAnnotations()).anyMatch(a -> a instanceof InputQueue))
            .forEach(f -> processInputQueueAnnotatedField(f, testInstance));
    }

    private void processOutputQueueAnnotatedField(Field field, Object testInstance) {
        try {
            log.trace("Start to process field: {} of object: {}", field, testInstance);
            if (!field.getType().isAssignableFrom(BlockingQueue.class)) {
                throw new KafkaQueuesException("Wrong class, should be: " + BlockingQueue.class);
            }
            if (testConsumers.get(field.getName()) != null) {
                log.debug("Test consumer already configured");
                field.setAccessible(true);
                field.set(testInstance, testConsumers.get(field.getName()).getRecordsQueue());
            } else {
                final OutputQueue annotation = field.getAnnotation(OutputQueue.class);
                KafkaOutputQueueProvider tk = consumer(annotation);
                tk.start();
                testConsumers.put(field.getName(), tk);
                final BlockingQueue<ConsumerRecord<Object, Object>> recordsQueue = tk.getRecordsQueue();
                field.setAccessible(true);
                field.set(testInstance, recordsQueue);
                log.trace("Finish processing for object: {}, field: {}", testInstance, field);
            }
        } catch (Exception e) {
            throw new KafkaQueuesException("Failed to process annotated field", e);
        }
    }

    private void processInputQueueAnnotatedField(Field field, Object testInstance) {
        try {
            log.trace("Start to process field: {} of object: {}", field, testInstance);
            if (!field.getType().isAssignableFrom(BlockingQueue.class)) {
                throw new KafkaQueuesException("Wrong class, should be: " + BlockingQueue.class);
            }
            if (testProducers.get(field.getName()) != null) {
                log.debug("Test producer already configured");
                field.setAccessible(true);
                field.set(testInstance, testProducers.get(field.getName()).getRecordsQueue());
            } else {
                final InputQueue annotation = field.getAnnotation(InputQueue.class);
                KafkaInputQueueProvider tp = producer(annotation);
                testProducers.put(field.getName(), tp);
                final BlockingQueue<ProducerRecord<Object, Object>> recordsQueue = tp.getRecordsQueue();
                field.setAccessible(true);
                field.set(testInstance, recordsQueue);
                log.trace("Finish processing for object: {}, field: {}", testInstance, field);
            }
        } catch (Exception e) {
            throw new KafkaQueuesException("Failed to process annotated field", e);
        }
    }

    private KafkaOutputQueueProvider consumer(OutputQueue annotation) {
        final String[] additionalProperties = annotation.additionalProperties();
        final String bootstrapServers = annotation.bootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("BootstrapServers is absent");
        }
        final String servers = resolveProperty(bootstrapServers);
        final int partitions = annotation.partitions();
        final String topic = annotation.topic();
        final Class<?> keyDes = annotation.keyDeserializer();
        final Class<?> valDes = annotation.valueDeserializer();
        KafkaOutputQueueProvider testKafkaConsumer;
        if (additionalProperties.length == 0) {
            testKafkaConsumer = KafkaOutputQueueProvider.consumer(
                topic,
                servers,
                partitions,
                keyDes,
                valDes
            );
        } else {
            testKafkaConsumer = KafkaOutputQueueProvider.consumerWithAdditionalProperties(
                topic,
                servers,
                partitions,
                keyDes,
                valDes,
                Arrays.stream(additionalProperties).map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> resolveProperty(s[1])))
            );
        }
        return testKafkaConsumer;
    }

    private KafkaInputQueueProvider producer(InputQueue annotation) {
        final String[] additionalProperties = annotation.additionalProperties();
        final String bootstrapServers = annotation.bootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("BootstrapServers is absent");
        }
        final String servers = resolveProperty(bootstrapServers);
        final Class<?> keyDes = annotation.keySerializer();
        final Class<?> valDes = annotation.valueSerializer();
        KafkaInputQueueProvider testKafkaProducer;
        if (additionalProperties.length == 0) {
            testKafkaProducer = KafkaInputQueueProvider.producer(
                servers,
                keyDes,
                valDes
            );
        } else {
            testKafkaProducer = KafkaInputQueueProvider.producer(
                servers,
                keyDes,
                valDes,
                Arrays.stream(additionalProperties).map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> resolveProperty(s[1])))
            );
        }
        return testKafkaProducer;
    }

    private String resolveProperty(String property) {
        log.trace("Start to resolve property: {}", property);
        String resolved;
        if (property.startsWith("${") && property.endsWith("}")) {
            resolved = System.getProperty(property.replaceAll("[\\$, \\{,\\}]", ""));
            if (resolved == null) {
                throw new IllegalArgumentException("Failed to resolve property from: " + property);
            }
        } else {
            resolved = property;
        }
        log.trace("Resolved property: {}", resolved);
        return resolved;
    }
}
