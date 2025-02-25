package org.example;


import org.example.apache_http_client.ApacheHttpExecute;
import org.example.grpc.TestClient;
import org.example.grpc.TestServer;
import org.example.grpc.TestServiceGrpc;
import org.example.jedis.JedisExecute;
import org.example.jetty_servlet.JettyServer;
import org.example.kafka_consumer.KafkaConsumerExecute;
import org.example.kafka_consumer.KafkaProducerExecute;
import org.example.mongo.MongoDB;
import org.example.mysql.MySQLJDBCExample;

import java.io.IOException;

import static java.lang.Thread.sleep;

public class Main {
     public static void main(String[] args) throws Exception {
         final String EXECUTION = "grpc";

         switch (EXECUTION) {
             case "grpc" -> {
                 new Thread(() -> {
                     try {
                         TestServer.main(null);
                     } catch (Exception e) {
                         throw new RuntimeException(e);
                     }
                 });
                 TestClient.main(null);
             }

             case "kafka" -> {
                new Thread(() -> KafkaConsumerExecute.main(null));
                for(int i = 0; i < 100; i ++) {
                    KafkaProducerExecute.main(null);
                }
             }

             case "redis" -> JedisExecute.main(null);

             case "servlet" -> JettyServer.main(null);

             case "mongodb" -> MongoDB.main(null);

             case "mysql" -> MySQLJDBCExample.main(null);
         }

    }

}