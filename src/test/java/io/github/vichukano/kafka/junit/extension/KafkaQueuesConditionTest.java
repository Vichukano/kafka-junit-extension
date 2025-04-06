package io.github.vichukano.kafka.junit.extension;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@EnableKafkaQueues
@EmbeddedKafka(topics = {"first", "second", "third", "fourth"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaQueuesConditionTest {
    @OutputQueue(topic = "first")
    private BlockingQueue<ConsumerRecord<String, String>> stringRecordsQueue;
    @OutputQueue(
            topic = "second",
            keyDeserializer = IntegerDeserializer.class,
            valueDeserializer = IntegerDeserializer.class,
            bootstrapServers = "${bootstrap-servers}",
            partitions = 1
    )
    private BlockingQueue<ConsumerRecord<Integer, Integer>> intRecordsQueue;
    @InputQueue
    private BlockingQueue<ProducerRecord<String, String>> thirdInputQueue;
    @OutputQueue(topic = "third")
    private BlockingQueue<ConsumerRecord<String, String>> thirdOutputQueue;
    @InputQueue(
            additionalProperties = {"retries=1"},
            bootstrapServers = "${bootstrap-servers}",
            keySerializer = IntegerSerializer.class,
            valueSerializer = IntegerSerializer.class
    )
    private BlockingQueue<ProducerRecord<Integer, Integer>> fourthInputQueue;
    @OutputQueue(
            topic = "fourth",
            additionalProperties = {"enable.auto.commit=false"},
            keyDeserializer = IntegerDeserializer.class,
            valueDeserializer = IntegerDeserializer.class
    )
    private BlockingQueue<ConsumerRecord<Integer, Integer>> fourthOutputQueue;

    @BeforeAll
    void init() {
        final String bs = System.getProperty("spring.embedded.kafka.brokers");
        System.setProperty("bootstrap-servers", bs);
    }

    @Test
    void shouldPollRecordFromStringRecordsQueue() throws ExecutionException, InterruptedException, TimeoutException {
        Assertions.assertThat(stringRecordsQueue.isEmpty()).isTrue();
        final String key = "foo";
        final String payload = "bar";
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("spring.embedded.kafka.brokers"));
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
        template.setDefaultTopic("first");

        template.sendDefault(key, payload).get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> record = stringRecordsQueue.poll(10, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.key()).isEqualTo(key);
        Assertions.assertThat(record.value()).isEqualTo(payload);
    }

    @Test
    void shouldPollRecordFromIntRecordsQueue() throws ExecutionException, InterruptedException, TimeoutException {
        Assertions.assertThat(intRecordsQueue.isEmpty()).isTrue();
        final Integer key = 1;
        final Integer payload = 2;
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("spring.embedded.kafka.brokers"));
        DefaultKafkaProducerFactory<Integer, Integer> pf = new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<Integer, Integer> template = new KafkaTemplate<>(pf, true);
        template.setDefaultTopic("second");

        template.sendDefault(key, payload).get(10, TimeUnit.SECONDS);

        ConsumerRecord<Integer, Integer> record = intRecordsQueue.poll(10, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.key()).isEqualTo(key);
        Assertions.assertThat(record.value()).isEqualTo(payload);
    }

    @Test
    void shouldSendRecordsFromInputQueAndPollFromOutput() throws InterruptedException {
        var key = "key";
        var value = "value";

        thirdInputQueue.offer(
                new ProducerRecord<>("third", key, value), 10, TimeUnit.SECONDS
        );

        ConsumerRecord<String, String> record = thirdOutputQueue.poll(10, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.key()).isEqualTo(key);
        Assertions.assertThat(record.value()).isEqualTo(value);
    }

    @Test
    void shouldSuccessfullySendAndReceiveRecordsWithAdditionalProperties() throws InterruptedException {
        var key = 1;
        var value = 2;

        fourthInputQueue.offer(
                new ProducerRecord<>("fourth", key, value), 10, TimeUnit.SECONDS
        );

        ConsumerRecord<Integer, Integer> record = fourthOutputQueue.poll(10, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.key()).isEqualTo(key);
        Assertions.assertThat(record.value()).isEqualTo(value);
    }
}
