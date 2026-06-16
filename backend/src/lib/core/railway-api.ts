// Small helpers for Railway's GraphQL metrics API.
// Used by railway-metrics.ts (post-hoc report) and idle-multi.ts (chart
// memory/CPU after a sharded run).

import { readFileSync } from "fs";
import { homedir } from "os";

export function readRailwayToken(): string {
  if (process.env.RAILWAY_TOKEN) return process.env.RAILWAY_TOKEN;
  // Fall back to the file the `railway` CLI writes after `railway login`.
  const cfg = JSON.parse(readFileSync(`${homedir()}/.railway/config.json`, "utf-8"));
  return cfg.user.token;
}

export interface DataPoint {
  ts: number; // unix seconds
  value: number;
}

export interface FetchMetricArgs {
  token: string;
  projectId: string;
  serviceId: string;
  measurement: "MEMORY_USAGE_GB" | "CPU_USAGE";
  startDate: string;
  endDate: string;
  // Railway enforces a minimum sampleRateSeconds — empirically 30s for
  // recent windows. Larger windows can use a coarser rate.
  sampleRate?: number;
}

export async function fetchMetric(args: FetchMetricArgs): Promise<DataPoint[]> {
  const query = `
    query Metrics($projectId: String!, $serviceId: String!, $start: DateTime!, $end: DateTime!, $measurement: MetricMeasurement!, $sampleRate: Int!) {
      metrics(
        projectId: $projectId
        serviceId: $serviceId
        startDate: $start
        endDate: $end
        measurements: [$measurement]
        sampleRateSeconds: $sampleRate
      ) {
        measurement
        values { ts value }
      }
    }
  `;
  const res = await fetch("https://backboard.railway.com/graphql/v2", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${args.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      variables: {
        projectId: args.projectId,
        serviceId: args.serviceId,
        start: args.startDate,
        end: args.endDate,
        measurement: args.measurement,
        sampleRate: args.sampleRate ?? 30,
      },
    }),
  });
  const json = (await res.json()) as {
    data?: { metrics?: Array<{ values: DataPoint[] }> };
    errors?: unknown;
  };
  if (json.errors) {
    console.error(
      `Failed to fetch ${args.measurement}:`,
      JSON.stringify(json.errors, null, 2)
    );
    return [];
  }
  return json.data?.metrics?.[0]?.values ?? [];
}
