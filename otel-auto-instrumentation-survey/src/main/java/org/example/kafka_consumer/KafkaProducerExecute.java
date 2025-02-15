package org.example.kafka_consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerExecute {
    public static void main(String[] args) throws InterruptedException {
        String[] topics = {"topic-1", "topic-2", "topic-3", "topic-4", "topic-5"};
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        for (int i = 0; i < 3000000; i++) {
            String topic = topics[random.nextInt(topics.length)];
            String key = "key-" + random.nextInt(1000000);
            String value = "value-" + random.nextInt(1000000);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            Thread.sleep(10);
        }
        producer.close();
    }
}
