const fs = require('node:fs');
const { NodeTracerProvider } = require('@opentelemetry/sdk-trace-node');
const { BatchSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const { trace, ROOT_CONTEXT } = require('@opentelemetry/api');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const path = require('path');

// 处理命令行参数
const args = process.argv.slice(2);
const configPathArg = args.find(arg => arg.startsWith('--config='));
const configPath = configPathArg ? configPathArg.split('=')[1] : 'config.json';
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));

if(!config) {
  console.log('config json file is needed')
  process.exit()
}


const collectorOptions = {
  url: config.otel.url,
};

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let cnt = 0;

const exporter = new OTLPTraceExporter(collectorOptions);

const tracerProvider = new NodeTracerProvider();
tracerProvider.addSpanProcessor(new BatchSpanProcessor(exporter, {
  maxExportBatchSize: config.otel.batchSize,
  exportTimeoutMillis: config.otel.timeout
}));
tracerProvider.register();

const tracer = trace.getTracer('manual-tracing');
const timestampBase = new Date().getTime();

async function readFile(filePath) {
  const buffer = fs.readFileSync(filePath);
  const lines = buffer.toString().split('\n');
  console.log(lines.length);
  const attributes = lines.shift().split(',');
  for (let i = 0; i <= 30000; i += 512) {
    lines.slice(i, Math.min(i + 512, 30000)).forEach(line => {
      createSpan(attributes, line.split(','));
    });
    await timeout(40);
  }
}

readFile(config.path);

function createSpan(keys, values) {
  if (keys.length != values.length) return;
  const span = tracer.startSpan('alibaba-bench', {
    startTime: timestampBase + parseInt(values[0], 10)
  }, ROOT_CONTEXT);
  values.forEach((value, index) => {
    span.setAttribute(keys[index], value);
  });
  // span.addEvent('test-event', {
  //   'does-it': 'works?',
  //   'i-hope-it': 'works!'
  // })
  span.end(timestampBase + parseInt(values[0], 10) + parseInt(values[10], 10));
}
