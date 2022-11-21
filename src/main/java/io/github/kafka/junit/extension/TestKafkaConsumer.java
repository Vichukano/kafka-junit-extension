package io.github.kafka.junit.extension;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
class TestKafkaConsumer {
    @Getter
    private final BlockingQueue<ConsumerRecord<Object, Object>> recordsQueue;
    private final KafkaMessageListenerContainer<Object, Object> listenerContainer;
    private final int partitions;

    private TestKafkaConsumer(BlockingQueue<ConsumerRecord<Object, Object>> recordsQueue,
                              KafkaMessageListenerContainer<Object, Object> listenerContainer,
                              int partitions) {
        this.recordsQueue = recordsQueue;
        this.listenerContainer = listenerContainer;
        this.partitions = partitions;
    }

    public void start() {
        log.debug("Start test kafka consumer");
        listenerContainer.start();
        ContainerTestUtils.waitForAssignment(listenerContainer, partitions);
        log.debug("Test kafka consumer ready to consume records");
    }

    public void stop() {
        log.debug("Stop test kafka consumer");
        listenerContainer.stop();
        log.debug("Test kafka consumer stopped");
    }

    public static TestKafkaConsumer consumer(String topic,
                                             String bootstrapServers,
                                             int partitions,
                                             Class<?> keyDeserializerClass,
                                             Class<?> valueDeserializerClass
    ) {
        log.trace("Start to create test consumer for topic: {}", topic);
        var queue = new LinkedBlockingQueue<ConsumerRecord<Object, Object>>();
        var consumerProps = commonProps(bootstrapServers, keyDeserializerClass, valueDeserializerClass);
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        var containerProps = new ContainerProperties(topic);
        var listenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        listenerContainer.setupMessageListener((MessageListener<Object, Object>) queue::add);
        var testConsumer = new TestKafkaConsumer(queue, listenerContainer, partitions);
        log.trace("Created test consumer: {}", testConsumer);
        return testConsumer;
    }

    public static TestKafkaConsumer consumerWithAdditionalProperties(String topic,
                                                                     String bootstrapServers,
                                                                     int partitions,
                                                                     Class<?> keyDeserializerClass,
                                                                     Class<?> valueDeserializerClass,
                                                                     Map<String, Object> props) {
        log.trace("Start to create test consumer for topic: {} and props: {}", topic, props);
        var testConsumer = consumer(
            topic,
            bootstrapServers,
            partitions,
            keyDeserializerClass, valueDeserializerClass
        );
        props.forEach((k, v) -> testConsumer.listenerContainer
            .getContainerProperties()
            .getKafkaConsumerProperties()
            .setProperty(k, String.valueOf(v)));
        log.trace("Created test consumer: {}", testConsumer);
        return testConsumer;
    }

    private static Map<String, Object> commonProps(String bootstrapServers,
                                                   Class<?> keyDeserializerClass,
                                                   Class<?> valueDeserializerClass) {
        var consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return consumerProps;
    }
}
