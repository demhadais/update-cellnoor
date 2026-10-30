import { describe, expect, test } from "bun:test";
import {
  buildLibrary,
  buildLibraryMeasurements,
  indexSetFields,
  type LibraryRow,
} from "./libraries.ts";

describe("indexSetFields", () => {
  const singleIndexSetNames = new Set(["SI-NA-A1"]);

  test("a single index set is recognized by name", () => {
    expect(indexSetFields("SI-NA-A1", singleIndexSetNames)).toEqual({
      single_index_set_name: "SI-NA-A1",
      dual_index_set_name: null,
    });
  });

  test("anything else is a dual index set", () => {
    expect(indexSetFields("SI-TT-H7", singleIndexSetNames)).toEqual({
      single_index_set_name: null,
      dual_index_set_name: "SI-TT-H7",
    });
  });
});

describe("buildLibrary", () => {
  const row: LibraryRow = {
    readable_id: "25E32-L3",
    cdna_id: "25E32-C1",
    index_set_name: "SI-TT-H7",
    number_of_sample_index_pcr_cycles: 10,
    target_reads_per_cell: 50,
    prepared_at: "2025-05-29T00:00:00.000Z",
    preparers: ["ahmed.said@jax.org"],
    notes: "cDNA input: 456ng",
    concentration: 41650,
    mean_size_bp: 543,
    sizing_range_minimum: 150,
    sizing_range_maximum: 1000,
    qubit_concentration: 27.3,
  };
  const ids = {
    people: new Map([["ahmed.said@jax.org", "person"]]),
    cdna: new Map([["25E32-C1", "cdna"]]),
    singleIndexSetNames: new Set<string>(),
  };

  test("target reads per cell are converted from thousands", () => {
    expect(buildLibrary(row, ids).target_reads_per_cell).toBe(50000);
  });

  test("TapeStation and Qubit measurements are both sent", () => {
    expect(
      buildLibraryMeasurements(row, "person").map((m) => m.data.type),
    ).toEqual(["Electrophoretic", "Fluorometric"]);
  });
});
