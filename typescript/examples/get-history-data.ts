import { createClient, decodeReport, LogLevel } from "../src";
import { getReportVersion, formatReport } from "../src/utils/report";
import { getCurrentTimestamp, validateTimestampWithin30Days } from "../src/utils/time";
import { ZmqPublisher } from "../src/utils/zeromq";
import "dotenv/config";

/**
 * Parse a UTC date-time string (e.g. "2026-03-11 00:00:00") to Unix timestamp in seconds.
 * If the string is a plain integer, treat it as a Unix timestamp directly.
 */
function parseTimeArg(arg: string): number {
  const asInt = Number(arg);
  if (!isNaN(asInt) && String(Math.floor(asInt)) === arg.trim()) {
    return asInt;
  }
  const date = new Date(arg.trim().replace(" ", "T") + "Z");
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid time format: "${arg}". Expected "YYYY-MM-DD HH:MM:SS" (UTC) or Unix timestamp.`);
  }
  return Math.floor(date.getTime() / 1000);
}

/**
 * Get today's UTC 00:00:00 as Unix timestamp in seconds.
 */
function getTodayUtcStartTimestamp(): number {
  const now = new Date();
  const utcStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  return Math.floor(utcStart.getTime() / 1000);
}

async function main() {
  if (process.argv.length < 3) {
    console.error("Usage: npx ts-node examples/get-history-data.ts <feedIds> [startTime] [endTime]");
    console.error("\nArguments:");
    console.error("  feedIds    - Comma-separated feed IDs");
    console.error('  startTime  - Optional. UTC datetime string (e.g. "2026-03-10 12:00:00") or Unix timestamp. Default: today UTC 00:00:00');
    console.error("  endTime    - Optional. Same format as startTime. Default: current time");
    console.error("\nExamples:");
    console.error("  npx ts-node examples/get-history-data.ts 0x0003...btc,0x0003...eth");
    console.error('  npx ts-node examples/get-history-data.ts 0x0003...btc "2026-03-10 12:00:00" "2026-03-11 00:00:00"');
    process.exit(1);
  }

  const feedIds = process.argv[2].split(",");
  const startTime = process.argv[3] ? parseTimeArg(process.argv[3]) : getTodayUtcStartTimestamp();
  const endTime = process.argv[4] ? parseTimeArg(process.argv[4]) : getCurrentTimestamp();

  validateTimestampWithin30Days(startTime);

  if (startTime >= endTime) {
    console.error(`Error: startTime (${startTime}) must be before endTime (${endTime})`);
    process.exit(1);
  }

  const PAGE_SIZE = 100;

  const zmqEndpoint = process.env.ZMQ_ENDPOINT || "tcp://127.0.0.1:5555";
  const pub = new ZmqPublisher({ endpoint: zmqEndpoint, sendHighWaterMark: 1000 });

  try {
    await pub.connect();
    console.log(`ZMQ publisher connected to ${zmqEndpoint}`);

    const config = {
      apiKey: process.env.API_KEY || "YOUR_API_KEY",
      userSecret: process.env.USER_SECRET || "YOUR_USER_SECRET",
      endpoint: "https://api.dataengine.chain.link",
      wsEndpoint: "wss://ws.dataengine.chain.link",
      logging: {
        logger: console,
        logLevel: LogLevel.INFO,
      },
    };

    const client = createClient(config);

    let grandTotal = 0;

    for (const feedId of feedIds) {
      const version = getReportVersion(feedId);
      console.log(
        `\nFetching reports for feed ${feedId} (${version}), time range: ${startTime} → ${endTime}...\n`
      );

      let currentStartTime = startTime;
      let feedTotal = 0;
      let reportIndex = 0;

      while (currentStartTime < endTime) {
        const reports = await client.getReportsPage(feedId, currentStartTime, PAGE_SIZE);

        for (const report of reports) {
          // Stop if we've reached the end time
          if (report.observationsTimestamp > endTime) break;

          reportIndex++;
          console.log(`Raw Report Blob #${reportIndex}: ${report.fullReport}`);

          const decodedData = decodeReport(report.fullReport, report.feedID);
          const decodedReport = {
            ...decodedData,
            feedID: report.feedID,
            validFromTimestamp: report.validFromTimestamp,
            observationsTimestamp: report.observationsTimestamp,
          };
          console.log(formatReport(decodedReport, version));

          await pub.publish("chain-link-history", JSON.stringify(decodedReport, (_, v) => typeof v === "bigint" ? v.toString() : v));
          console.log(`  -> Published to ZMQ topic "chain-link-history"`);

          feedTotal++;
        }

        if (reports.length < PAGE_SIZE) {
          break;
        }

        const lastReport = reports[reports.length - 1];
        if (lastReport.observationsTimestamp >= endTime) {
          break;
        }

        currentStartTime = lastReport.observationsTimestamp + 1;
        console.log(`\nFetched ${feedTotal} reports for this feed, fetching next page from timestamp ${currentStartTime}...\n`);
      }

      console.log(`\nCompleted feed ${feedId}: ${feedTotal} reports.`);
      grandTotal += feedTotal;
    }

    console.log(`\nAll done. Total ${grandTotal} reports published across ${feedIds.length} feed(s).`);
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
