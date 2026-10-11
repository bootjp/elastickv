import { render, screen } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it } from "vitest";

import type { KeyVizRow } from "../api/client";
import { parseBucketID, RowDetail, subRangeLabel } from "./KeyViz";

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

describe("RowDetail", () => {
  const row: KeyVizRow = {
    bucket_id: "route:7#0",
    start: "",
    end: "",
    aggregate: false,
    route_count: 1,
    values: [3],
    sub_bucket_count: 4,
  };

  it("shows the bucket-zero sub-range pill, explanation, and narrowed bounds", () => {
    render(createElement(RowDetail, { row, index: 0 }));

    expect(screen.getByText("sub-range 1/4")).toHaveAttribute(
      "title",
      expect.stringContaining("narrowed bounds of this sub-range"),
    );
    expect(screen.getByText("Sub-range start")).toBeInTheDocument();
    expect(screen.getByText("Sub-range end")).toBeInTheDocument();
  });

  it("uses whole-route bounds without a sub-range pill", () => {
    render(createElement(RowDetail, {
      row: { ...row, bucket_id: "route:7", sub_bucket_count: 1 },
      index: 0,
    }));

    expect(screen.queryByText(/sub-range \d+\/\d+/)).not.toBeInTheDocument();
    expect(screen.getByText("Start")).toBeInTheDocument();
    expect(screen.getByText("End")).toBeInTheDocument();
  });
});

describe("subRangeLabel", () => {
  const row = (over: Partial<KeyVizRow>): KeyVizRow => ({
    bucket_id: "route:7",
    start: "",
    end: "",
    aggregate: false,
    route_count: 1,
    values: [],
    ...over,
  });

  it("labels a sub-divided route as 1-based i/K", () => {
    expect(subRangeLabel(row({ sub_bucket: 0, sub_bucket_count: 8 }))).toBe("sub-range 1/8");
    expect(subRangeLabel(row({ sub_bucket: 7, sub_bucket_count: 8 }))).toBe("sub-range 8/8");
  });

  // The index is omitted from the JSON for bucket zero, so the label
  // must key off the COUNT. Testing sub_bucket instead would hide the
  // first sub-range of every route.
  it("labels bucket zero even though the index is absent from the wire", () => {
    expect(subRangeLabel(row({ sub_bucket_count: 4 }))).toBe("sub-range 1/4");
  });

  it("returns null when the route is not sub-divided", () => {
    expect(subRangeLabel(row({}))).toBeNull();
    expect(subRangeLabel(row({ sub_bucket_count: 0 }))).toBeNull();
    expect(subRangeLabel(row({ sub_bucket_count: 1 }))).toBeNull();
  });
});
