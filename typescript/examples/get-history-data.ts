import { createClient, decodeReport, LogLevel } from "../src";
import { getReportVersion, formatReport } from "../src/utils/report";
import { getCurrentTimestamp, getThirtyDaysAgoTimestamp, validateTimestampWithin30Days } from "../src/utils/time";
import { ZmqPublisher } from "../src/utils/zeromq";
import "dotenv/config";

async function main() {
  if (process.argv.length < 2) {
    console.error("Please provide a feed ID");
    console.error(
      "Example: npx ts-node examples/get-history-data.ts 0x000359843a543ee2fe414dc14c7e7920ef10f4372990b79d6361cdc0dd1ba782"
    );
    console.error(`Current timestamp: ${getCurrentTimestamp()}`);
    process.exit(1);
  }

  const feedId = process.argv[2];
  const startTime = process.argv[3] ? parseInt(process.argv[3]) : getThirtyDaysAgoTimestamp();
  validateTimestampWithin30Days(startTime);
  const limit = process.argv[4] ? parseInt(process.argv[4]) : 30 * 24 * 60 * 60;
  const version = getReportVersion(feedId);

  const zmqEndpoint = process.env.ZMQ_ENDPOINT || "tcp://127.0.0.1:5556";
  const pub = new ZmqPublisher({ endpoint: zmqEndpoint, sendHighWaterMark: 1000 });

  try {
    await pub.bind();
    console.log(`ZMQ publisher bound to ${zmqEndpoint}`);

    const config = {
      apiKey: process.env.API_KEY || "YOUR_API_KEY",
      userSecret: process.env.USER_SECRET || "YOUR_USER_SECRET",
      endpoint: "https://api.dataengine.chain.link",
      wsEndpoint: "wss://ws.dataengine.chain.link",
      // Comment to disable SDK logging:
      logging: {
        logger: console,
        logLevel: LogLevel.INFO,
      },
    };

    const client = createClient(config);
    console.log(
      `\nFetching reports for feed ${feedId} (${version}) starting from timestamp ${startTime}${limit ? ` (limit: ${limit})` : ""}...\n`
    );

    const reports = await client.getReportsPage(feedId, startTime, limit);
    console.log(`Found ${reports.length} reports:\n`);

    for (const [index, report] of reports.entries()) {
      console.log(`Raw Report Blob #${index + 1}: ${report.fullReport}`);

      // Decode the report
      const decodedData = decodeReport(report.fullReport, report.feedID);

      // Combine decoded data with report metadata
      const decodedReport = {
        ...decodedData,
        feedID: report.feedID,
        validFromTimestamp: report.validFromTimestamp,
        observationsTimestamp: report.observationsTimestamp,
      };
      console.log(formatReport(decodedReport, version));

      // Publish decodedReport to ZMQ queue
      await pub.publish("chain-link-data", JSON.stringify(decodedReport));
      console.log(`  -> Published to ZMQ topic "chain-link-data"`);
    }

    console.log(`\nAll ${reports.length} reports published to ZMQ.`);
  } catch (error) {
    if (error instanceof Error) {
      console.error("Error:", error.message);
    } else {
      console.error("Unknown error:", error);
    }
    process.exit(1);
  } finally {
    await pub.close();
  }
}

main();
