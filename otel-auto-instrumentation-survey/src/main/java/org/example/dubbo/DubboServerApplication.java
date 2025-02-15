package org.example.dubbo;



import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableDubbo
@Configuration
public class DubboServerApplication {

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(DubboServerApplication.class);
        context.start();
        System.out.println("Dubbo server started.");
    }

    @Bean
    public GreetingServiceImpl greetingService() {
        return new GreetingServiceImpl();
    }
}