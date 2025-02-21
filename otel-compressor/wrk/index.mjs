import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';

// Get terminal input arguments
const args = process.argv.slice(2);
const folderPath = path.resolve(args[0] || "spans"); // First argument: folder path, default is "spans"
const mergeFileSize = parseInt(args[1] || "1", 10); // Second argument: merge size, default is 1
const endpointURL = args[2] || "http://127.0.0.1:4318/v1/traces"; // Third argument: endpoint URL, default is OpenTelemetry endpoint
const sendInterval = parseInt(args[3] || "1000", 10); // Fourth argument: send interval, default is 1000 ms

// Get all .txt files in the folder
const files = fs.readdirSync(folderPath).filter(file => file.endsWith('.txt'));

// Delay function
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function sendFiles() {
  const startTime = Date.now(); // Start time
  let totalSize = 0; // Total size of files sent (in bytes)

  let toSend = null;
  let count = 0;
  let fileSizeCount = 0;

  const sending = async () => {
    for (const file of files) {
      const filePath = path.join(folderPath, file);
      try {
        // Get file size
        const fileStats = fs.statSync(filePath);
        const fileSize = fileStats.size; // File size in bytes
        fileSizeCount += fileSize;

        // Read file content
        const data = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(data); // Assume the file content is valid JSON

        if (count++ === 0 && mergeFileSize !== 1) {
          toSend = jsonData;
          continue;
        }
        toSend.resource_spans.push(...(jsonData.resource_spans));

        if (count !== mergeFileSize) {
          continue;
        }

        count = 0;

        const bodyString = JSON.stringify(toSend);

        totalSize += bodyString.length;
        fileSizeCount = 0;

        // Send data to the specified endpoint
        const response = await fetch(endpointURL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: bodyString, // Send the merged file content
        });

        toSend = null;

        if (!response.ok) {
          console.log(`Failed to send: ${response.statusText}`);
        }

        // Calculate throughput after each send
        const elapsedTime = (Date.now() - startTime) / 1000; // Convert to seconds
        const throughput = totalSize / elapsedTime; // Calculate throughput (bytes/second)

        // Output current statistics
        console.log(`Total size sent: ${formatSize(totalSize)}.`);
        console.log(`Current throughput: ${formatSize(throughput)}/second.`);

      } catch (err) {
        console.error(`Error reading or sending ${file}: ${err.message}`);
      }

      // Wait 1000 milliseconds before sending the next request
      await delay(sendInterval); // 1000 ms delay
    }
  };

  await sending();
}

// Format file size into a readable unit (bytes, KB, MB, GB)
function formatSize(size) {
  if (size < 1024) return `${size} bytes`;
  else if (size < 1024 * 1024) return `${(size / 1024).toFixed(2)} KB`;
  else if (size < 1024 * 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(2)} MB`;
  else return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

sendFiles();