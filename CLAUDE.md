# Working on this benchmark repo

Read `docs/methodology.md` first; `docs/railway-ops.md` for fleet mechanics. When working from the anycable-web repo, the full playbook lives in `.claude/skills/benchmarking/` there.

## Rules that are never optional

1. **Run `npm run bench:preflight` (from `backend/`) before any paid run window.** It verifies runner image freshness, secrets parity across shards, target env, and deploy churn. `npm run bench:fleet` shows what is live and billing.
2. **The load generator is a suspect in every result.** Keep ~250 cables per runner for latency/jitter, ~1 runner per 10K connections for capacity. Multi-shard drivers self-flag validity problems (delivery over 100%, uniform shard ceilings, negative skew floor, elapsed overrun); a fatal flag means fix and rerun, never publish.
3. **Never ship a load-generator-limited number.** Identical per-shard ceilings with a healthy server = the fleet's wall, not the server's.
4. **Same-window comparisons only.** Every row of one table comes from one continuous window, zero deploy churn during runs; re-run outliers in isolation.
5. **Fairness before running:** worker counts from boot logs (stock puma.rb ignores `WEB_CONCURRENCY` without a `workers` directive), equal box sizes set explicitly, native client per adapter for reconnect-dependent tests, same enforced outage for every client.
6. **Teardown ends every window**: `railway down` or `deploymentRemove` per service (never `deploymentStop`; limits changes alone keep billing), verify domains return 404, restore any temporarily altered config. The fleet-watchdog Action opens an issue if something is left running.
7. **Baselines live in `src/bench/tests-manifest.ts`**; update them in the same commit as the change that moved them. New query params must be registered in `src/bench-runner/known-params.ts` or every driver sending them fails fast (by design).
8. After a bench-runner code change, `railway up` every shard in use and verify deployment `createdAt` postdates the commit; `railway redeploy` reuses the old image and git push rebuilds nothing.

## Writing style (README, docs, PR bodies)

Succinct, alive, no throat-clearing. No em dashes. State the positive directly (no "not X, but Y" constructions). Keep caveats attached to the numbers they qualify; a public table must never look cleaner than the underlying data.
