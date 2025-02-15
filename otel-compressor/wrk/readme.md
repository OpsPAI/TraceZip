# Manual Tracing with OpenTelemetry

This repository provides a Node.js script to read a CSV file, create tracing spans using OpenTelemetry, and export the traces to an OpenTelemetry collector. The configuration for the OpenTelemetry collector, timeout, batch size, and file path is specified in a configuration file.

**Special Notice**: We do not recommend directly using the Alibaba microservice cluster tracing dataset for generating workload, since the microservice cluster tracing dataset contains globally collected spans data. In this case, using a static compressor will yield better compression benefits. Directly using the dataset for generating workload will only result in a 10% compression improvement with our OTEL compressor compared to using only gzip. If you attempt to use the Alibaba microservice cluster tracing dataset for generating workload and compressing it with our OTEL compressor, you should filter the spans in the dataset that are located at the same link nodes in the Call Graph Topology.

## Prerequisites

- Node.js installed on your system.
- The following Node.js packages installed:
  - `@opentelemetry/sdk-trace-node`
  - `@opentelemetry/sdk-trace-base`
  - `@opentelemetry/api`
  - `@opentelemetry/exporter-trace-otlp-http`

## Installation

1. Install the required packages:

   ```bash
   npm install
   ```

## Configuration

Create a configuration file (`config.json`) in the root directory of the repository. The configuration file should have the following structure:

```json
{
  "otel": {
    "url": "http://localhost:4318/v1/traces",
    "timeout": 5000,
    "batchSize": 20000
  },
  "path": "[file path]"
}
```

- `otel.url`: The URL of the OpenTelemetry collector.
- `otel.timeout`: Timeout for exporting spans in milliseconds.
- `otel.batchSize`: Maximum batch size for exporting spans.
- `path`: Path to the CSV file to be processed.

## Usage

To run the script, use the following command:

```bash
node main.js --config=config.json
```

If you want to use a different configuration file, specify the path to the new configuration file:

```bash
node main.js --config=path/to/your/config.json
```

## Script Explanation

The script reads the configuration file, processes the CSV file specified in the `path` configuration, and sends tracing data to the specified OpenTelemetry collector. It creates spans for each line in the CSV file, setting attributes based on the CSV columns.

### Example CSV Format

The CSV file should have the following format (example):

```csv
timestamp,traceid,rpc,duration,attr1,attr2,attr3
1234567890,abc123,exampleRpc,1000,value1,value2,value3
```

### Script Details

- Reads the CSV file specified in the configuration.
- Creates spans for each line, excluding specified attributes (`timestamp`, `traceid`, `rpc`).
- Exports spans to the OpenTelemetry collector with the specified batch size and timeout.

## License

This project is licensed under the MIT License.
