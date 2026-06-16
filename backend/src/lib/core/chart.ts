// Tiny ASCII line-chart helper. No deps. Single series at a time.
// Use it to print a quick-glance memory or CPU curve in the console.
//
//   chart({
//     title: "Memory (MB)",
//     points: [{ tSec: 0, value: 280 }, { tSec: 30, value: 1980 }, ...],
//     height: 12,
//     width: 60,
//     yUnit: "MB",
//   });

export interface ChartPoint {
  tSec: number;
  value: number;
}

export interface ChartOptions {
  title: string;
  points: ChartPoint[];
  height?: number;
  width?: number;
  yUnit?: string;
  yMin?: number; // override auto-fit
  yMax?: number;
}

export function chart(opts: ChartOptions): string {
  const points = opts.points.slice().sort((a, b) => a.tSec - b.tSec);
  if (points.length === 0) return `${opts.title}\n  (no data)`;

  const height = opts.height ?? 12;
  const width = opts.width ?? 60;
  const yUnit = opts.yUnit ?? "";

  const values = points.map((p) => p.value);
  const yMin = opts.yMin ?? Math.min(...values, 0);
  const yMax = opts.yMax ?? Math.max(...values);
  const yRange = yMax - yMin || 1;

  const tMin = points[0].tSec;
  const tMax = points[points.length - 1].tSec;
  const tRange = tMax - tMin || 1;

  // Build a rectangular grid (height rows × width columns) of ' '.
  // For each point, snap to (col, row) and write '*'. Connect with '-'
  // along intermediate columns at the closer row to avoid spikes.
  const grid: string[][] = Array.from({ length: height }, () =>
    Array<string>(width).fill(" ")
  );

  // Snap each data point.
  const snapped: { col: number; row: number }[] = points.map((p) => {
    const col = Math.min(
      width - 1,
      Math.round(((p.tSec - tMin) / tRange) * (width - 1))
    );
    const row = Math.min(
      height - 1,
      height - 1 - Math.round(((p.value - yMin) / yRange) * (height - 1))
    );
    return { col, row };
  });

  // Draw segments between successive points by stepping per column.
  for (let i = 0; i < snapped.length - 1; i++) {
    const a = snapped[i];
    const b = snapped[i + 1];
    const cols = Math.max(1, b.col - a.col);
    for (let dc = 0; dc <= cols; dc++) {
      const t = cols === 0 ? 0 : dc / cols;
      const r = Math.round(a.row + (b.row - a.row) * t);
      const c = a.col + dc;
      if (c < width && r >= 0 && r < height) grid[r][c] = "•";
    }
  }
  // Ensure each anchor point is marked even after segment overrides.
  for (const s of snapped) {
    if (s.col < width && s.row >= 0 && s.row < height) grid[s.row][s.col] = "●";
  }

  // Render with axis labels.
  const out: string[] = [];
  out.push(opts.title);

  const labelMax = formatY(yMax) + (yUnit ? ` ${yUnit}` : "");
  const labelMin = formatY(yMin) + (yUnit ? ` ${yUnit}` : "");
  const labelW = Math.max(labelMax.length, labelMin.length);

  for (let r = 0; r < height; r++) {
    let label = "";
    if (r === 0) label = labelMax.padStart(labelW);
    else if (r === height - 1) label = labelMin.padStart(labelW);
    else label = "".padStart(labelW);
    out.push(`  ${label} │ ${grid[r].join("")}`);
  }

  // X-axis ticks.
  const axis = "─".repeat(width);
  out.push(`  ${"".padStart(labelW)} └${axis}`);
  const tStartLabel = `${formatT(tMin)}s`;
  const tEndLabel = `${formatT(tMax)}s`;
  const padCenter = Math.max(0, width - tStartLabel.length - tEndLabel.length);
  out.push(`  ${"".padStart(labelW)}   ${tStartLabel}${" ".repeat(padCenter)}${tEndLabel}`);

  return out.join("\n");
}

function formatY(v: number): string {
  if (v >= 100) return v.toFixed(0);
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function formatT(s: number): string {
  return s.toFixed(0);
}
