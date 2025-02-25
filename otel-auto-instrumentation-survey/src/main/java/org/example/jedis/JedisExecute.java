package org.example.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JedisExecute {
    public static void main(String[] args) throws InterruptedException {
        // 初始化 50 个 key
        String[] keys = new String[50];
        for (int i = 0; i < 50; i++) {
            keys[i] = UUID.randomUUID().toString() + (new Date()).getTime();
        }

        Random random = new Random();

        // 配置 JedisPool 连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000); // 最大连接数
        poolConfig.setMaxIdle(100);   // 最大空闲连接数
        poolConfig.setMinIdle(10);    // 最小空闲连接数
        poolConfig.setTestOnBorrow(true); // 从池中获取连接时测试连接是否可用
        poolConfig.setTestWhileIdle(true); // 空闲时测试连接是否可用

        // 创建 JedisPool 连接池
        JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6379);

        // 创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(1000);

        // 提交任务
        for (int i = 0; i < 10000000; i++) {
            executorService.execute(() -> {
                // 随机选择一个 key
                String key = keys[random.nextInt(keys.length)];

                // 从 100 以内的整数中选择一个值作为 value
                String value = String.valueOf(random.nextInt(100));

                // 从连接池中获取 Jedis 实例
                try (Jedis jedis = jedisPool.getResource()) {
                    // 设置键值对
                    jedis.set(key, value);

                    // 获取键值对
                    String retrievedValue = jedis.get(key);

                    // 输出结果
                    System.out.println("The value of '" + key + "' is: " + retrievedValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // 控制任务提交速度（可选）
             Thread.sleep(10); // 如果需要限制任务提交速度，可以启用这行代码
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

        // 关闭 JedisPool
        jedisPool.close();
    }
}
