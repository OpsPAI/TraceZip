package org.example.kafka_consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaProducerExecute {
    public static void main(String[] args) throws InterruptedException {
        String[] topics = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};
        var keys = new ArrayList<String>();
        for(var i = 0; i < 20; i++) {
            keys.add(UUID.randomUUID().toString());
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        for (int i = 0; i < 3000000; i++) {
            String topic = topics[random.nextInt(topics.length)];
            String key = keys.get(random.nextInt(keys.size()));
            String value = "value-" + random.nextInt(1000000);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            Thread.sleep(1);
        }
        producer.close();
    }
}
