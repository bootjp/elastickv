// Five-stop perceptual ramp used by the KeyViz heatmap. The endpoint
// at t=0 is fully transparent so cold cells fall through to the page
// background — that distinction (cold vs. just-quiet) is the single
// most important read from the heatmap.
//
// Stops are RGBA tuples; ramp() linearly interpolates between the two
// adjacent stops for any input in [0, 1]. The ramp is intentionally
// hand-rolled rather than pulled from d3-interpolate so the SPA stays
// dependency-free for Phase 2-B.

type RGBA = readonly [number, number, number, number];

const stops: ReadonlyArray<readonly [number, RGBA]> = [
  [0.0, [0, 0, 0, 0]],
  [0.15, [56, 88, 222, 180]],
  [0.45, [86, 196, 110, 220]],
  [0.75, [240, 200, 60, 235]],
  [1.0, [220, 50, 50, 245]],
];

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

// ramp clamps `t` into [0, 1] and returns the interpolated RGBA tuple.
// NaN and negative inputs collapse to the t=0 stop so a divide-by-zero
// in the caller (empty matrix) produces transparent cells rather than
// rendering noise.
export function ramp(t: number): RGBA {
  if (!Number.isFinite(t) || t <= 0) return stops[0][1];
  if (t >= 1) return stops[stops.length - 1][1];
  for (let i = 1; i < stops.length; i++) {
    const [pos, color] = stops[i];
    if (t <= pos) {
      const [prevPos, prevColor] = stops[i - 1];
      const span = pos - prevPos;
      const local = span === 0 ? 0 : (t - prevPos) / span;
      return [
        Math.round(lerp(prevColor[0], color[0], local)),
        Math.round(lerp(prevColor[1], color[1], local)),
        Math.round(lerp(prevColor[2], color[2], local)),
        Math.round(lerp(prevColor[3], color[3], local)),
      ] as const;
    }
  }
  return stops[stops.length - 1][1];
}
