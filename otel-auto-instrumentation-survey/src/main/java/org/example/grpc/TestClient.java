package org.example.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Random;
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

    public void testServices() {
        Random random = new Random();
        String[] options = {"option1", "option2", "option3", "option4"};

        for (int i = 0; i < 100; i++) {
            String randomParam = "random" + random.nextInt(100);
            String param1 = options[random.nextInt(options.length)];
            String param2 = options[random.nextInt(options.length)];
            String param3 = randomParam;

            Example.ServiceRequest request = Example.ServiceRequest.newBuilder()
                    .setParam1(param1)
                    .setParam2(param2)
                    .setParam3(param3)
                    .build();

            int serviceNumber = random.nextInt(5) + 1;
            Example.ServiceResponse response;

            switch (serviceNumber) {
                case 1:
                    response = blockingStub.service1(request);
                    break;
                case 2:
                    response = blockingStub.service2(request);
                    break;
                case 3:
                    response = blockingStub.service3(request);
                    break;
                case 4:
                    response = blockingStub.service4(request);
                    break;
                case 5:
                    response = blockingStub.service5(request);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + serviceNumber);
            }

            System.out.println("Response from Service" + serviceNumber + ": " + response.getResult());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();
        int numClients = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        // 创建并发任务
        for (int i = 0; i < 1000; i++) {
            executorService.submit(() -> {
                String host = "mock" + random.nextInt(10) + ".local";
                int port = 8080;
                TestClient client = new TestClient(host, port);

                try {
                    client.testServices();
                } finally {
                    try {
                        client.shutdown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        // 关闭线程池
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(100000);  // 等待所有任务完成
        }
    }

}
