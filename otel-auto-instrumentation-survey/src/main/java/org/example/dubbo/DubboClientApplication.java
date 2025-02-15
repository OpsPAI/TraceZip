package org.example.dubbo;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;

import java.util.UUID;

public class DubboClientApplication {

    public static void main(String[] args) {
        ApplicationConfig application = new ApplicationConfig();
        application.setName("dubbo-client");

        RegistryConfig registry = new RegistryConfig();
        registry.setAddress("N/A");  // 使用直连模式，不使用注册中心

        ReferenceConfig<GreetingService> reference = new ReferenceConfig<>();
//        reference.setApplication(application);
        reference.setRegistry(registry);
        reference.setInterface(GreetingService.class);
        reference.setUrl("dubbo://localhost:20880");

        GreetingService greetingService = reference.get();
        String message = greetingService.sayHello(UUID.randomUUID().toString());
        System.out.println(message);
    }
}
