// Fleet teardown watchdog. Runs on a GitHub Actions cron (see
// .github/workflows/fleet-watchdog.yml) and checks whether any Railway
// service in the bench project still has a live deployment. Railway bills
// per-minute for allocated resources and the fleet is supposed to be torn
// down after every campaign; a forgotten fleet burns money silently.
//
// Behavior:
//   - live services found  → open (or update) an issue labeled fleet-watchdog
//     listing them, so somebody tears the fleet down or acknowledges a
//     campaign in progress by keeping the issue open.
//   - nothing live         → close any open fleet-watchdog issue.
//
// Env (set in the workflow):
//   RAILWAY_TOKEN       account token (create at railway.com/account/tokens;
//                       CLI login tokens expire, account tokens persist)
//   PROJECT_ID          Railway project uuid
//   ENVIRONMENT_ID      Railway environment uuid
//   GITHUB_TOKEN        provided by Actions (needs issues: write)
//   GITHUB_REPOSITORY   owner/repo, provided by Actions
//
// No dependencies: plain fetch, runs on any Node >= 20.

const {
  RAILWAY_TOKEN,
  PROJECT_ID,
  ENVIRONMENT_ID,
  GITHUB_TOKEN,
  GITHUB_REPOSITORY,
} = process.env;

for (const [name, v] of Object.entries({
  RAILWAY_TOKEN,
  PROJECT_ID,
  ENVIRONMENT_ID,
  GITHUB_TOKEN,
  GITHUB_REPOSITORY,
})) {
  if (!v) {
    console.error(`Missing env var: ${name}`);
    process.exit(1);
  }
}

const LIVE = new Set(["SUCCESS", "DEPLOYING", "BUILDING", "INITIALIZING", "WAITING"]);
const LABEL = "fleet-watchdog";

async function railway(query, variables) {
  const res = await fetch("https://backboard.railway.com/graphql/v2", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RAILWAY_TOKEN}`,
      "Content-Type": "application/json",
      "User-Agent": "curl/8.4.0",
      "Accept-Encoding": "identity",
    },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors) {
    throw new Error(`Railway GraphQL error: ${JSON.stringify(json.errors)}`);
  }
  return json.data;
}

async function github(method, path, body) {
  const res = await fetch(`https://api.github.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "fleet-watchdog",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`GitHub ${method} ${path}: HTTP ${res.status} ${await res.text()}`);
  }
  return res.status === 204 ? null : res.json();
}

const data = await railway(
  `query Env($id: String!) {
    environment(id: $id) {
      serviceInstances {
        edges { node { serviceName latestDeployment { status createdAt } } }
      }
    }
  }`,
  { id: ENVIRONMENT_ID },
);

const services = data.environment.serviceInstances.edges.map((e) => e.node);
const live = services.filter(
  (s) => s.latestDeployment && LIVE.has(s.latestDeployment.status),
);

console.log(`${services.length} services, ${live.length} live`);
for (const s of live) {
  console.log(`  live: ${s.serviceName} (${s.latestDeployment.status}, since ${s.latestDeployment.createdAt})`);
}

const issues = await github(
  "GET",
  `/repos/${GITHUB_REPOSITORY}/issues?state=open&labels=${LABEL}`,
);
const existing = issues[0];

if (live.length === 0) {
  if (existing) {
    await github("POST", `/repos/${GITHUB_REPOSITORY}/issues/${existing.number}/comments`, {
      body: "Fleet is fully torn down; closing.",
    });
    await github("PATCH", `/repos/${GITHUB_REPOSITORY}/issues/${existing.number}`, {
      state: "closed",
    });
    console.log(`Closed issue #${existing.number}`);
  } else {
    console.log("Fleet down, no open issue. Nothing to do.");
  }
  process.exit(0);
}

const oldest = live
  .map((s) => new Date(s.latestDeployment.createdAt))
  .sort((a, b) => a - b)[0];
const hoursUp = ((Date.now() - oldest.getTime()) / 3.6e6).toFixed(1);

const body = [
  `**${live.length} Railway service(s) are live and billing** (oldest deployment up ~${hoursUp}h).`,
  "",
  "If a benchmark campaign is running, keep this issue open as the reminder and close it after teardown. Otherwise: tear the fleet down (stop deployments per service; limits changes alone keep billing) and verify public domains return 404.",
  "",
  "| Service | Status | Deployed |",
  "|---|---|---|",
  ...live.map(
    (s) => `| ${s.serviceName} | ${s.latestDeployment.status} | ${s.latestDeployment.createdAt} |`,
  ),
  "",
  `_Updated ${new Date().toISOString()} by the fleet watchdog._`,
].join("\n");

if (existing) {
  await github("PATCH", `/repos/${GITHUB_REPOSITORY}/issues/${existing.number}`, { body });
  console.log(`Updated issue #${existing.number}`);
} else {
  const issue = await github("POST", `/repos/${GITHUB_REPOSITORY}/issues`, {
    title: "Bench fleet is live — tear down or acknowledge",
    body,
    labels: [LABEL],
  });
  console.log(`Opened issue #${issue.number}`);
}
