package org.example.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestClient {

    private final ManagedChannel channel;
    private final TestServiceGrpc.TestServiceBlockingStub blockingStub;

    public TestClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = TestServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void testServices() throws InterruptedException {
        Random random = new Random();
        String[] options = {"option1", "option2", "option3", "option4"}; // 补上缺失的定义

        // 参数关联规则（param2 必须与 param1 匹配）
        Map<String, List<String>> paramRelations = new HashMap<>() {{
            put("option1", Arrays.asList("option3", "option4"));
            put("option2", Arrays.asList("option1", "option4"));
            put("option3", Arrays.asList("option2", "option4"));
            put("option4", Arrays.asList("option1", "option2"));
        }};

        // 服务权重配置
        Map<Integer, Integer> serviceWeights = new HashMap<>() {{
            put(1, 30);
            put(2, 25);
            put(3, 20);
            put(4, 15);
            put(5, 10);
        }};

        for (int i = 0; i < 100; i++) {
            // 生成关联参数 ------------------------
            String param1 = options[random.nextInt(options.length)];

            // param2 基于 param1 的关联规则选择
            List<String> allowedParams = paramRelations.get(param1);
            String param2 = allowedParams.get(random.nextInt(allowedParams.size()));

            // param3 包含前两个参数的关联特征
            String param3 = String.format("%s-%s-%d", param1, param2, random.nextInt(10));

            // 构建请求对象 ------------------------
            Example.ServiceRequest request = Example.ServiceRequest.newBuilder()
                    .setParam1(param1)
                    .setParam2(param2)
                    .setParam3(param3)
                    .build();

            // 基于权重的服务选择 ------------------
            int serviceNumber = getWeightedRandom(serviceWeights, random);

            // 服务调用逻辑
            Example.ServiceResponse response = switch (serviceNumber) {
                case 1 -> blockingStub.service1(request);
                case 2 -> blockingStub.service2(request);
                case 3 -> blockingStub.service3(request);
                case 4 -> blockingStub.service4(request);
                case 5 -> blockingStub.service5(request);
                default -> throw new IllegalStateException("Unexpected service: " + serviceNumber);
            };

            System.out.printf("[Test #%03d] Service%d | Params: %s/%s | Result: %s\n",
                    i+1, serviceNumber, param1, param2, response.getResult());
            Thread.sleep(10);
        }
    }

    // 权重随机选择方法（保持不变）
    private int getWeightedRandom(Map<Integer, Integer> weights, Random random) {
        int totalWeight = weights.values().stream().mapToInt(Integer::intValue).sum();
        int randomNumber = random.nextInt(totalWeight);

        int cumulative = 0;
        for (Map.Entry<Integer, Integer> entry : weights.entrySet()) {
            cumulative += entry.getValue();
            if (randomNumber < cumulative) {
                return entry.getKey();
            }
        }
        return 1; // fallback
    }

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();
        int numClients = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        // 创建并发任务
        for (int i = 0; i < 1000000; i++) {
            executorService.submit(() -> {
                String host = "mock" + random.nextInt(10) + ".local";
                int port = 8080;
                TestClient client = new TestClient(host, port);

                try {
                    try {
                        client.testServices();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    try {
                        client.shutdown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        Thread.sleep(100000000);  // 等待所有任务完成

        // 关闭线程池
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(100000000);  // 等待所有任务完成
        }
    }

}
