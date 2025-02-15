package org.example.dubbo;

import org.apache.dubbo.config.annotation.DubboService;
@DubboService
public class GreetingServiceImpl implements GreetingService {
    @Override
    public String sayHello(String name) {
        return "Hello, " + name;
    }
}