import { describe, expect, it } from "vitest";

import { parseBucketID } from "./KeyViz";

describe("parseBucketID", () => {
  it("parses legacy and labeled route bucket IDs", () => {
    expect(parseBucketID("route:7")).toEqual({ kind: "route", routeID: 7 });
    expect(parseBucketID("route:7#3")).toEqual({ kind: "route", routeID: 7, subBucket: 3 });
    expect(parseBucketID("route:7:redis")).toEqual({ kind: "route", routeID: 7, label: "redis" });
    expect(parseBucketID("route:7:redis#3")).toEqual({ kind: "route", routeID: 7, label: "redis", subBucket: 3 });
  });

  it("rejects malformed labeled route bucket IDs", () => {
    expect(parseBucketID("route:7:")).toBeNull();
    expect(parseBucketID("route:7:redis:extra")).toBeNull();
    expect(parseBucketID("route:7:redis#")).toBeNull();
  });
});
