// Railway GraphQL helpers for fleet inspection (preflight / fleet diff /
// watchdog). Read-only: nothing here mutates services.
//
// Two operational rules learned the hard way, encoded here:
//   - never swallow GraphQL errors (an auth expiry once masqueraded as
//     rate limiting for an hour because stderr went to /dev/null) — every
//     helper throws with the raw error payload;
//   - Railway's schema drifts (the CLI's own `scale` subcommand panics on
//     it), so callers should catch and report, not assume.

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { readRailwayToken } from "./railway-api.js";

const ENDPOINT = "https://backboard.railway.com/graphql/v2";

export async function gql<T>(
  query: string,
  variables: Record<string, unknown>,
  token: string,
): Promise<T> {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      // Railway's edge rejects unfamiliar user agents (Python's default UA
      // gets a Cloudflare 403); curl's is known-good.
      "User-Agent": "curl/8.4.0",
      // Ask for uncompressed responses: Railway's edge otherwise sends
      // gzip that this fetch path does not always decompress (the
      // railway-metrics helper crashed on exactly this).
      "Accept-Encoding": "identity",
    },
    body: JSON.stringify({ query, variables }),
  });
  const text = await res.text();
  let json: { data?: T; errors?: unknown };
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(
      `Railway GraphQL returned non-JSON (HTTP ${res.status}): ${text.slice(0, 300)}`,
    );
  }
  if (json.errors) {
    throw new Error(
      `Railway GraphQL error: ${JSON.stringify(json.errors).slice(0, 500)}\n` +
        `(auth expired? run 'railway login'; schema drift? print the raw error, do not guess)`,
    );
  }
  if (!json.data) {
    throw new Error(`Railway GraphQL returned no data (HTTP ${res.status})`);
  }
  return json.data;
}

export interface FleetManifest {
  project: string;
  projectId?: string;
  environment: string;
  environmentId?: string;
  runnerPattern: string;
  excludedRunners: string[];
  sharedSecrets: string[];
  targets: Record<
    string,
    {
      role: string;
      expectEnv: string[];
      // Env values that make a run silently invalid (e.g. broadcast
      // adapter nats): key → forbidden value.
      forbidEnv?: Record<string, string>;
      notes: string;
    }
  >;
}

export function readManifest(path: string): FleetManifest {
  return JSON.parse(readFileSync(path, "utf-8")) as FleetManifest;
}

export interface ResolvedProject {
  projectId: string;
  environmentId: string;
}

// Resolve project + environment ids. Precedence: PROJECT_ID/ENVIRONMENT_ID
// env vars, then the ids pinned in fleet-manifest.json (the project lives
// in a team workspace, so the account-level `projects` listing does not
// see it), verified against the live project name.
export async function resolveProject(
  manifest: FleetManifest,
  token: string,
): Promise<ResolvedProject> {
  if (process.env.PROJECT_ID && process.env.ENVIRONMENT_ID) {
    return {
      projectId: process.env.PROJECT_ID,
      environmentId: process.env.ENVIRONMENT_ID,
    };
  }
  if (!manifest.projectId || !manifest.environmentId) {
    throw new Error(
      "fleet-manifest.json needs projectId + environmentId (or set PROJECT_ID + ENVIRONMENT_ID). Find them: railway status --json | jq '{id, env: .environments.edges[0].node.id}'",
    );
  }
  const data = await gql<{ project: { id: string; name: string } }>(
    `query P($id: String!) { project(id: $id) { id name } }`,
    { id: manifest.projectId },
    token,
  );
  if (data.project.name !== manifest.project) {
    throw new Error(
      `projectId ${manifest.projectId} resolves to "${data.project.name}", manifest says "${manifest.project}" — fix the manifest`,
    );
  }
  return {
    projectId: manifest.projectId,
    environmentId: manifest.environmentId,
  };
}

export interface ServiceState {
  serviceId: string;
  serviceName: string;
  deployment: {
    id: string;
    status: string;
    createdAt: string;
  } | null;
}

export async function listServiceStates(
  resolved: ResolvedProject,
  token: string,
): Promise<ServiceState[]> {
  const data = await gql<{
    environment: {
      serviceInstances: {
        edges: Array<{
          node: {
            serviceId: string;
            serviceName: string;
            latestDeployment: {
              id: string;
              status: string;
              createdAt: string;
            } | null;
          };
        }>;
      };
    };
  }>(
    `query Env($id: String!) {
      environment(id: $id) {
        serviceInstances {
          edges {
            node {
              serviceId
              serviceName
              latestDeployment { id status createdAt }
            }
          }
        }
      }
    }`,
    { id: resolved.environmentId },
    token,
  );
  return data.environment.serviceInstances.edges.map((e) => ({
    serviceId: e.node.serviceId,
    serviceName: e.node.serviceName,
    deployment: e.node.latestDeployment,
  }));
}

// A deployment counts as live (billing) when its latest deployment is in a
// running-ish state. REMOVED = torn down; FAILED/CRASHED are not billing
// but are also not healthy.
const LIVE_STATUSES = new Set(["SUCCESS", "DEPLOYING", "BUILDING", "INITIALIZING", "WAITING"]);
const CHURN_STATUSES = new Set(["DEPLOYING", "BUILDING", "INITIALIZING", "WAITING"]);

export function isLive(s: ServiceState): boolean {
  return s.deployment !== null && LIVE_STATUSES.has(s.deployment.status);
}

export function isChurning(s: ServiceState): boolean {
  return s.deployment !== null && CHURN_STATUSES.has(s.deployment.status);
}

// Decrypted service variables. Never print values — hash them (sha12) for
// cross-service comparison.
export async function getServiceVariables(
  resolved: ResolvedProject,
  serviceId: string,
  token: string,
): Promise<Record<string, string>> {
  const data = await gql<{ variables: Record<string, string> }>(
    `query Vars($projectId: String!, $environmentId: String!, $serviceId: String!) {
      variables(projectId: $projectId, environmentId: $environmentId, serviceId: $serviceId)
    }`,
    {
      projectId: resolved.projectId,
      environmentId: resolved.environmentId,
      serviceId,
    },
    token,
  );
  return data.variables ?? {};
}

export function sha12(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}

export { readRailwayToken };
