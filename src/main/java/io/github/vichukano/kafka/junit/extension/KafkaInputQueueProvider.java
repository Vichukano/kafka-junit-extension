package io.github.vichukano.kafka.junit.extension;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class KafkaInputQueueProvider {
    @Getter
    private final BlockingQueue<ProducerRecord<Object, Object>> recordsQueue;
    private final Producer<Object, Object> producer;

    private KafkaInputQueueProvider(Producer<Object, Object> producer) {
        this.producer = producer;
        this.recordsQueue = new ProducerRecordsInputQueue(new LinkedBlockingQueue<>(), this.producer);
    }

    static KafkaInputQueueProvider producer(String bootstrapServers,
                                            Class<?> keySerializerClass,
                                            Class<?> valueSerializerClass) {
        Properties props = commonProps(bootstrapServers, keySerializerClass, valueSerializerClass);
        return createProvider(props);
    }

    static KafkaInputQueueProvider producer(String bootstrapServers,
                                            Class<?> keySerializerClass,
                                            Class<?> valueSerializerClass,
                                            Map<String, Object> additionalProps) {
        Properties props = commonProps(bootstrapServers, keySerializerClass, valueSerializerClass);
        props.putAll(additionalProps);
        return createProvider(props);
    }

    private static KafkaInputQueueProvider createProvider(Properties properties) {
        log.trace("Start to create producer provider with properties: {}", properties);
        var producer = new KafkaProducer<>(properties);
        var provider = new KafkaInputQueueProvider(producer);
        log.trace("Created test producer provider: {}", provider);
        return provider;
    }

    private static Properties commonProps(String bootstrapServers,
                                          Class<?> keySerializerClass,
                                          Class<?> valueSerializerClass) {
        var consumerProps = new Properties();
        consumerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        consumerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        consumerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return consumerProps;
    }

    void stop() {
        log.debug("Start to stop provider");
        producer.close();
        log.debug("Provider stopped");
    }
}
