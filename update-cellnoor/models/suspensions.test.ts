import { describe, expect, test } from "bun:test";
import {
  buildSuspension,
  buildSuspensionMeasurements,
  type SuspensionRow,
} from "./suspensions.ts";

const people = new Map([
  ["preparer@jax.org", "preparer"],
  ["submitter@jax.org", "submitter"],
]);

function suspension(overrides: Partial<SuspensionRow>): SuspensionRow {
  return {
    readable_id: "25E1-S1",
    specimen_id: "25SP1",
    content: "Nuclei",
    created_at: "2025-02-04T00:00:00.000Z",
    lysis_duration_minutes: null,
    target_cell_recovery: null,
    preparers: ["preparer@jax.org"],
    notes: null,
    counting_method: "AOPI",
    customer_measured_by: "submitter@jax.org",
    customer_concentration: null,
    customer_volume: null,
    customer_viability: null,
    concentration: null,
    volume: null,
    viability: null,
    mean_diameter: null,
    adjusted_concentration: null,
    adjusted_volume: null,
    adjusted_viability: null,
    adjusted_mean_diameter: null,
    post_hybridization_concentration: null,
    post_hybridization_volume: null,
    post_hybridization_mean_diameter: null,
    ...overrides,
  };
}

describe("buildSuspensionMeasurements", () => {
  test("each group is attributed and flagged correctly", () => {
    const measurements = buildSuspensionMeasurements(
      suspension({
        customer_volume: 1250,
        concentration: 394000,
        post_hybridization_mean_diameter: 12.3,
      }),
      "2025-02-04T00:00:00.000Z",
      people,
    );

    expect(measurements).toEqual([
      {
        measured_by: "submitter",
        measured_at: "2025-02-04T00:00:00.000Z",
        data: {
          quantity: "volume",
          value: 1250,
          unit: "microliter",
          post_hybridization: false,
        },
      },
      {
        measured_by: "preparer",
        measured_at: "2025-02-04T00:00:00.000Z",
        data: {
          quantity: "concentration",
          value: 394000,
          numerator_unit: "nuclei",
          denominator_unit: "milliliter",
          counting_method: "acridine_orange_propidium_iodide",
          post_hybridization: false,
        },
      },
      {
        measured_by: "preparer",
        measured_at: "2025-02-04T00:00:00.000Z",
        data: {
          quantity: "mean_diameter",
          value: 12.3,
          object: "nuclei",
          unit: "micrometer",
          post_hybridization: true,
        },
      },
    ]);
  });

  test("viability is converted from a percentage to a fraction", () => {
    const [measurement] = buildSuspensionMeasurements(
      suspension({ viability: 90.6 }),
      "2025-02-04T00:00:00.000Z",
      people,
    );

    expect(measurement?.data.quantity).toBe("viability");
    expect(measurement?.data.value).toBeCloseTo(0.906);
  });

  test("concentrations computed by formulas are sent as integers", () => {
    const [measurement] = buildSuspensionMeasurements(
      suspension({ concentration: 8140000.000000001 }),
      "2025-02-04T00:00:00.000Z",
      people,
    );

    expect(measurement?.data.value).toBe(8140000);
  });

  test("identical measurements are only sent once", () => {
    const measurements = buildSuspensionMeasurements(
      suspension({ volume: 50, adjusted_volume: 50 }),
      "2025-02-04T00:00:00.000Z",
      people,
    );

    expect(measurements).toHaveLength(1);
  });
});

describe("buildSuspension", () => {
  const ids = {
    people,
    specimens: new Map([
      ["25SP1", { id: "specimen", received_at: "2025-01-01T00:00:00.000Z" }],
    ]),
  };

  test("a missing creation date falls back to the specimen's received date", () => {
    expect(
      buildSuspension(suspension({ created_at: null }), ids).created_at,
    ).toBe("2025-01-01T00:00:00.000Z");
  });

  test("a target cell recovery computed by a formula is sent as an integer", () => {
    expect(
      buildSuspension(suspension({ target_cell_recovery: 3495.1 }), ids)
        .target_cell_recovery,
    ).toBe(3495);
  });
});
