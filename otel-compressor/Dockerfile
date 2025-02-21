FROM golang:latest AS builder

WORKDIR /otel

COPY . .

RUN go build -o /otel/gateway_collector ./otelcol-dev

FROM ubuntu:latest

WORKDIR /app

COPY --from=builder /otel/gateway_collector /app/gateway_collector
COPY --from=builder /otel/config/compressor-agent.yml /app/compressor-agent.yml

ENTRYPOINT [ "/app/gateway_collector" ]
CMD ["--config", "compressor-agent.yml"]

EXPOSE 4317
EXPOSE 4318
EXPOSE 8888
EXPOSE 55678
EXPOSE 55679
EXPOSE 6831
EXPOSE 6832
