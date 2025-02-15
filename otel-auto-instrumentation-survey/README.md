# Project Setup and Configuration

## Prerequisites
Before starting, make sure you have the following:
- JDK
- Apache Maven for dependency management
- OpenTelemetry Java Agent (if using telemetry)

## VM Options Configuration
To configure the Java VM with required settings, add the following VM options:

```bash
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.math=ALL-UNNAMED
```

These options ensure that necessary packages are accessible for instrumentation.

## OpenTelemetry Instrumentation
If you wish to use OpenTelemetry for distributed tracing, set the following environment variable to use the Java agent:

```bash
JAVA_TOOL_OPTIONS=-javaagent:/path/to/your/opentelemetry-javaagent.jar;
```

Make sure to replace `/path/to/your/opentelemetry-javaagent.jar` with the actual path of your OpenTelemetry Java Agent.

## Using Different Components

To run different components, modify the `Main.java` file located at:

```plaintext
src/main/java/org/example/Main.java
```

In `Main.java`, you can choose which component to execute by modifying the code that initializes the required service. The available components are:

### Available Components

- **Apache HTTP Client**:  
  Example class: `apache_http_client/ApacheHttpExecute.java`

- **Dubbo**:  
  Example classes: `dubbo/DubboClientApplication.java`, `dubbo/DubboServerApplication.java`

- **gRPC**:  
  Example classes: `grpc/TestClient.java`, `grpc/TestServer.java`

- **Jedis (Redis)**:  
  Example class: `jedis/JedisExecute.java`

- **Jetty Servlet**:  
  Example classes: `jetty_servlet/HelloServlet.java`, `jetty_servlet/JettyServer.java`

- **Kafka Consumer/Producer**:  
  Example classes: `kafka_consumer/KafkaConsumerExecute.java`, `kafka_consumer/KafkaProducerExecute.java`

- **MongoDB**:  
  Example class: `mongo/MongoDB.java`

- **MySQL JDBC**:  
  Example class: `mysql/MySQLJDBCExample.java`

### Modifying `Main.java`

You can switch between different components by modifying the `main` method in `Main.java`. For instance, to run the `Dubbo` component, you would import the appropriate classes and call the `DubboClientApplication.java` or `DubboServerApplication.java` methods.

Similarly, to use other components like `gRPC`, `Kafka`, or `MongoDB`, just replace the appropriate line in the `main` method with the corresponding class initialization.

## Maven Configuration
Ensure you have the necessary dependencies in your `pom.xml` to support the components you wish to use. If you are adding OpenTelemetry, include the required Maven dependencies for the OpenTelemetry Java agent.
