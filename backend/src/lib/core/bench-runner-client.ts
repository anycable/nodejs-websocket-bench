// Shared helpers for driver scripts that POST to a Railway-hosted
// bench-runner. The bench-runner enforces a bearer-token gate when
// BENCH_RUNNER_TOKEN is set on its side; the driver side reads the same
// env var and sends `Authorization: Bearer <token>` on every request.
// If the env var is unset, the helpers send no header (matches the old
// behavior when auth was off).

export function benchRunnerHeaders(
  extra: Record<string, string> = {},
): Record<string, string> {
  const token = process.env.BENCH_RUNNER_TOKEN;
  return token ? { ...extra, Authorization: `Bearer ${token}` } : { ...extra };
}

export function benchRunnerFetch(
  url: string,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(url, {
    ...init,
    headers: benchRunnerHeaders((init.headers as Record<string, string>) || {}),
  });
}
