# Send Files to OpenTelemetry Endpoint

This script reads `.txt` files from a specified folder, merges their contents up to a specified size, and sends the merged data to an OpenTelemetry endpoint. It also calculates and logs throughput statistics during the process.

## Requirements

- Node.js
- Dependencies: `node-fetch`

Install `node-fetch` using:
```bash
npm install node-fetch
```

## Usage

Run the script with:
```bash
node script.js <folderPath> <mergeFileSize> <endpointURL>
```

- `<folderPath>`: Path to the folder containing `.txt` files (default: `spans`).
- `<mergeFileSize>`: Number of files to merge in each batch (default: `20`).
- `<endpointURL>`: The OpenTelemetry endpoint to send data to (default: `http://127.0.0.1:4318/v1/traces`).

Specifically, if you want to send the dataset we provided, you need to categorize all the span data by hostname and send them accordingly. You can find the hostname in the attributes of the resource_spans in each spans file.

## Notes

1. The script assumes that `.txt` files contain valid JSON.
2. A delay of 1 second is added between each request.
3. The OpenTelemetry endpoint URL can be specified as a parameter.

## Example Output

When running the script, you will see logs like:
```
Total size sent: 1.23 MB.
Current throughput: 512.34 KB/second.
```

These logs indicate the total data sent and the current throughput.
