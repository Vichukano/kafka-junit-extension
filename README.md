# kafka-junit-extension

This library helps in testing applications that use Apache Kafka. 
Using the library, you can easily set up an outgoing queue that will receive messages that the application under test publishes to topics.
In order to enable the extension, it is necessary to mark the test class with the ```@EnableKafkaQueues``` annotation.
To enable an outgoing message queue, you need to declare a field with the ```BlockingQueue<ConsumerRecord<K, V>>``` type in the test class, 
where K is the key type and V is the message type, and mark it with the ```@OutputQueue``` annotation.

## Properties:

```@OutputQueue``` has some properties:
### mandatory
* topic - output topic, where application publish messages;
* bootstrapServers - kafka bootstrap servers. If this property has format like ```${bootstrap-servers}```, then it
  is takes from the environment. Default value is ```"${spring.embedded.kafka.brokers}```;
* keyDeserializer - type of key deserializer;
* valueDeserializer - type of value deserializer;
* partitions - number of topic partitions;
### optional
* additionalProperties - some custom kafka consumer properties, example = ```property-key=property-value```.


Usage [example](https://github.com/Vichukano/kafka-junit-extension/blob/main/src/test/java/io/github/vichukano/kafka/junit/extension/KafkaQueuesConditionTest.java)