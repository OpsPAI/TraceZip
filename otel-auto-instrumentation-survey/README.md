To run it, add `VM Options`: 
```
--add-opens
java.base/java.lang=ALL-UNNAMED
--add-opens
java.base/java.util=ALL-UNNAMED
--add-opens
java.base/java.nio=ALL-UNNAMED
--add-opens
java.base/sun.nio.ch=ALL-UNNAMED
--add-opens
java.base/java.math=ALL-UNNAMED
```

Otel Instrumentation Via Java Agent:
```
JAVA_TOOL_OPTIONS=-javaagent:/path/to/your/opentelemetry-javaagent.jar;
```