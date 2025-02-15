package org.example.jedis;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JedisExecute {
    public static void main(String[] args) {
        String[] keys = new String[50];
        for (int i = 0; i < 50; i++) {
            keys[i] = "key" + i;
        }

        Random random = new Random();
        int[] ports = {6379, 6397, 6973};
        String[] domains = {
                "mock0.local", "mock1.local", "mock2.local", "mock3.local", "mock4.local",
                "mock5.local", "mock6.local", "mock7.local", "mock8.local", "mock9.local"
        };

        // 创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(1000);

        for (int i = 0; i < 10000000; i++) {
            executorService.execute(() -> {
                // 随机选择一个 key
                String key = keys[random.nextInt(keys.length)];

                // 从 100 以内的整数中选择一个值作为 value
                String value = String.valueOf(random.nextInt(100));

                // 随机选择一个域名
                String domain = domains[random.nextInt(domains.length)];

                // 随机选择一个端口
                int port = ports[random.nextInt(ports.length)];

                try (Jedis jedis = new Jedis(domain, port)) {
                    // 设置键值对
                    jedis.set(key, value);

                    // 获取键值对
                    String retrievedValue = jedis.get(key);

                    // 输出结果
                    System.out.println("The value of '" + key + "' on " + domain + ":" + port + " is: " + retrievedValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // 关闭线程池
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
