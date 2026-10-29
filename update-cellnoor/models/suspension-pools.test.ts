import { describe, expect, test } from "bun:test";
import {
  buildPool,
  type PooledSuspensionRow,
  type SuspensionPoolRow,
} from "./suspension-pools.ts";

const suspensionIds = new Map([
  ["25E1-S1", "s1"],
  ["25E1-S2", "s2"],
]);

function pool(overrides: Partial<SuspensionPoolRow>): SuspensionPoolRow {
  return {
    readable_id: "M1",
    name: "DO EB Pool 10",
    multiplexing_tag_type: "Flex Barcode",
    pooled_at: "2025-02-06T00:00:00.000Z",
    preparers: ["ahmed.said@jax.org"],
    notes: null,
    pre_storage_concentration: null,
    pre_storage_volume: null,
    concentration: null,
    volume: null,
    viability: null,
    mean_diameter: null,
    ...overrides,
  };
}

const members: PooledSuspensionRow[] = [
  {
    "Pooled Into ID": "M1",
    readable_id: "25E1-S1",
    content: "Cells",
    multiplexing_tag_id: "BC001",
  },
  {
    "Pooled Into ID": "M1",
    readable_id: "25E1-S2",
    content: "Cells",
    multiplexing_tag_id: "BC002",
  },
];

describe("buildPool", () => {
  test("a tagged pool lists each suspension's tag", () => {
    expect(buildPool(pool({}), members, suspensionIds)).toEqual({
      multiplexing_tag_type: "flex_barcode",
      suspensions: [
        { suspension_id: "s1", tag_id: "BC001" },
        { suspension_id: "s2", tag_id: "BC002" },
      ],
    });
  });

  test("a pool without a tag type (genetic multiplexing) lists only IDs", () => {
    expect(
      buildPool(pool({ multiplexing_tag_type: null }), members, suspensionIds),
    ).toEqual({ suspensions: ["s1", "s2"] });
  });
});
