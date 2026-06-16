// Shared peak-RSS tracker for the bench runners.
//
// Why polling and not process.resourceUsage().maxRSS:
// maxRSS is the *process-lifetime* high-water mark and only goes up,
// so it doesn't reset between tests. The bench-runner is a long-lived
// HTTP server that handles back-to-back tests, where a 5 GB peak in
// test A would contaminate every subsequent test's reported peak.
// Polling RSS at 250 ms catches per-test peaks correctly. The CPU cost
// is negligible (~5 µs per process.memoryUsage() call).
//
// Why 250 ms and not 5 s: short tests can finish in 60 to 90 s, and a
// 5 s interval will routinely miss the actual peak (especially during
// the connect-storm ramp). 250 ms catches every observable spike.

const POLL_MS = 250;

export interface PeakRssHandle {
  // Stop polling and return the peak observed during the window, in MB.
  // Also factors in the very final RSS sample so a spike at the test's
  // closing moment isn't missed between the last poll and stop().
  stop: () => number;
}

export function trackPeakRss(): PeakRssHandle {
  let peakBytes = process.memoryUsage().rss;
  const handle = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > peakBytes) peakBytes = rss;
  }, POLL_MS);
  return {
    stop(): number {
      clearInterval(handle);
      const final = process.memoryUsage().rss;
      if (final > peakBytes) peakBytes = final;
      return peakBytes / 1024 / 1024;
    },
  };
}
