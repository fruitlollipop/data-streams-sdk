import { createClient, LogLevel } from "../src";
import { getReportVersion, formatReport } from "../src/utils/report";
import { decodeReport } from "../src/decoder";
import { ZmqPublisher } from "../src/utils/zeromq";
import "dotenv/config";

async function main() {
  if (process.argv.length < 3) {
    console.error("Please provide one or more feed IDs as arguments");
    console.error("\nExamples:");
    console.error("  Single feed:");
    console.error(
      "    npx ts-node examples/stream-reports.ts 0x000359843a543ee2fe414dc14c7e7920ef10f4372990b79d6361cdc0dd1ba782"
    );
    console.error("  Multiple feeds:");
    console.error(
      "    npx ts-node examples/stream-reports.ts 0x000359843a543ee2fe414dc14c7e7920ef10f4372990b79d6361cdc0dd1ba782,0x00036fe43f87884450b4c7e093cd5ed99cac6640d8c2000e6afc02c8838d0265"
    );
    console.error("  High Availability mode:");
    console.error("    npx ts-node examples/stream-reports.ts <feedIds> --ha");
    process.exit(1);
  }

  const feedIds = process.argv[2].split(",");
  const haMode = process.argv.includes("--ha");

  const zmqEndpoint = process.env.ZMQ_ENDPOINT || "tcp://127.0.0.1:5555";
  const pub = new ZmqPublisher({ endpoint: zmqEndpoint, sendHighWaterMark: 1000 });

  console.log("Chainlink Data Streams - Report Streaming");
  console.log("=".repeat(60));
  console.log(`📊 Feeds: ${feedIds.length} feed(s)`);
  console.log(`🎯 Mode: ${haMode ? "High Availability" : "Single Connection"}`);
  console.log("=".repeat(60));

  try {
    await pub.bind();
    console.log(`ZMQ publisher bound to ${zmqEndpoint}`);

    const client = createClient({
      apiKey: process.env.API_KEY || "YOUR_API_KEY",
      userSecret: process.env.USER_SECRET || "YOUR_USER_SECRET",
      endpoint: process.env.REST_URL || "https://api.dataengine.chain.link",
      wsEndpoint: process.env.WS_URL || "wss://ws.dataengine.chain.link",
      haMode,

      // Comment to disable SDK logging:
      logging: {
        logger: console,
        logLevel: LogLevel.INFO,
        enableConnectionDebug: false, // Enable WebSocket ping/pong and connection state logs (logLevel should be DEBUG)
      },
    });

    let reportCount = 0;

    // Create stream with custom options
    const stream = client.createStream(feedIds, {
      maxReconnectAttempts: 10,
      reconnectInterval: 3000,
    });

    // Event: Process incoming reports
    stream.on("report", async report => {
      reportCount++;

      try {
        console.log(`\n📈 Report #${reportCount} - ${new Date().toISOString()}`);

        // Show raw report blob
        console.log(`\nRaw Report Blob: ${report.fullReport}`);

        // Decode the report
        const decodedData = decodeReport(report.fullReport, report.feedID);
        const version = getReportVersion(report.feedID);

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
      } catch (error) {
        console.error(`❌ Error processing report: ${error instanceof Error ? error.message : error}`);
      }

      // Display stats every 5 reports
      if (reportCount % 5 === 0) {
        const stats = stream.getMetrics();
        console.log(
          `\n📊 Stats: ${stats.accepted} reports | ${stats.activeConnections}/${stats.configuredConnections} connections`
        );
      }
    });

    // Event: Handle errors
    stream.on("error", error => {
      console.error(`\n❌ Error: ${error.message}`);

      if (error.message.includes("authentication")) {
        console.error("💡 Check your API_KEY and USER_SECRET environment variables");
      }
    });

    // Event: Handle disconnections
    stream.on("disconnected", () => {
      console.log("\n🔴 Stream disconnected - reconnecting...");
    });

    // Event: Monitor reconnections
    stream.on("reconnecting", (info: { attempt: number; delayMs: number; origin?: string; host?: string }) => {
      console.log(
        `🔄 Reconnecting... attempt ${info.attempt} in ~${info.delayMs}ms${info.host ? ` (${info.host})` : ""}`
      );
    });

    console.log("⏳ Connecting...\n");
    await stream.connect();
    console.log("✅ Connected! Listening for reports...\n");

    // Graceful shutdown
    const shutdown = async () => {
      console.log("\n🛑 Shutting down...");
      await stream.close();
      await pub.close();
      console.log("✅ Shutdown complete");
      process.exit(0);
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  } catch (error) {
    console.error("❌ Failed to start stream:", error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

main();
