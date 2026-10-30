import { describe, expect, test } from "bun:test";
import { buildCdna, buildCdnaMeasurements, type CdnaRow } from "./cdna.ts";

const ids = {
  people: new Map([["ahmed.said@jax.org", "person"]]),
  gemWells: new Map([["G0078", "gem-well"]]),
};

function cdna(overrides: Partial<CdnaRow>): CdnaRow {
  return {
    readable_id: "25E32-C1",
    gem_well_id: "G0078",
    library_type: "Gene Expression",
    n_amplification_cycles: 11,
    prepared_at: "2025-05-16T00:00:00.000Z",
    preparers: ["ahmed.said@jax.org"],
    notes: null,
    concentration: 182500,
    sizing_range_minimum: 250,
    sizing_range_maximum: 5000,
    ...overrides,
  };
}

describe("buildCdna", () => {
  test("gene expression cDNA has a number of amplification cycles", () => {
    expect(buildCdna(cdna({}), ids)).toMatchObject({
      library_type: "gene_expression",
      n_amplification_cycles: 11,
    });
  });

  test("other cDNA doesn't", () => {
    const built = buildCdna(
      cdna({ library_type: "Chromatin Accessibility" }),
      ids,
    );
    expect(built.library_type).toBe("chromatin_accessibility");
    expect(built).not.toHaveProperty("n_amplification_cycles");
  });

  test("the TapeStation measurement is sent", () => {
    expect(buildCdnaMeasurements(cdna({}), "person")).toEqual([
      {
        measured_by: "person",
        measured_at: "2025-05-16T00:00:00.000Z",
        data: {
          type: "Electrophoretic",
          instrument_name: "TapeStation",
          concentration: {
            value: 182500,
            numerator_unit: "picogram",
            denominator_unit: "microliter",
          },
          sizing_range: [250, 5000],
          mean_size_bp: null,
        },
      },
    ]);
  });
});
