import { describe, expect, test } from "bun:test";
import {
  assayIds,
  buildChromiumRun,
  buildGemWell,
  type GemsRow,
  type GemsSuspensionRow,
} from "./chromium-runs.ts";

const ids = {
  people: new Map([["ahmed.said@jax.org", "person"]]),
  suspensions: new Map([
    ["25E32-S1", "s1"],
    ["25E32-S2", "s2"],
    ["25E32-S6", "s6"],
    ["25E38-S5", "s5"],
  ]),
  pools: new Map([["M75", "m75"]]),
  assays: new Map([
    ["Single Cell 3' Gene Expression OCM v4", "ocm-assay"],
    ["Single Cell 3' Gene Expression v4", "standard-assay"],
  ]),
};

function gems(overrides: Partial<GemsRow>): GemsRow {
  return {
    readable_id: "CR28",
    gem_well_readable_id: "G0078",
    assay_id: "Single Cell 3' Gene Expression OCM v4",
    run_at: "2025-05-13T00:00:00.000Z",
    run_by: "ahmed.said@jax.org",
    ...overrides,
  };
}

function loading(overrides: Partial<GemsSuspensionRow>): GemsSuspensionRow {
  return {
    "GEMs ID": "G0078",
    suspension_id: null,
    suspension_pool_id: null,
    ocm_barcode_id: [],
    failed: false,
    ilab_request_ids: ["SCB-JAD-1294 day 4"],
    ...overrides,
  };
}

describe("buildGemWell", () => {
  test("an OCM well gets one loading per barcode, without duplicate rows", () => {
    const row = loading({ suspension_id: "25E32-S6", ocm_barcode_id: ["OB3+OB4"] });

    expect(
      buildGemWell(
        "G0079",
        [row, row, loading({ suspension_id: "25E32-S1", ocm_barcode_id: ["OB1"] })],
        ids,
      ),
    ).toEqual({
      plexy: "on_chip_multiplexing",
      readable_id: "G0079",
      loading: [
        { ocm_barcode_id: "ob3", suspension_id: "s6" },
        { ocm_barcode_id: "ob4", suspension_id: "s6" },
        { ocm_barcode_id: "ob1", suspension_id: "s1" },
      ],
    });
  });

  test("a suspension pool can be loaded by OCM barcode", () => {
    expect(
      buildGemWell(
        "G0304",
        [loading({ suspension_pool_id: "M75", ocm_barcode_id: ["OB1"] })],
        ids,
      ),
    ).toEqual({
      plexy: "on_chip_multiplexing",
      readable_id: "G0304",
      loading: [{ ocm_barcode_id: "ob1", suspension_pool_id: "m75" }],
    });
  });

  test("a well without OCM barcodes is standard", () => {
    expect(
      buildGemWell(
        "G0091",
        [loading({ suspension_id: "25E38-S5", ocm_barcode_id: ["BC007"] })],
        ids,
      ),
    ).toEqual({ plexy: "standard", readable_id: "G0091", suspension_id: "s5" });
  });

  test("a standard well with more than one suspension is rejected", () => {
    expect(() =>
      buildGemWell(
        "G0091",
        [
          loading({ suspension_id: "25E38-S5" }),
          loading({ suspension_id: "25E32-S1" }),
        ],
        ids,
      ),
    ).toThrow();
  });
});

describe("buildChromiumRun", () => {
  const ocmRows = new Map([
    ["G0078", [loading({ suspension_id: "25E32-S1", ocm_barcode_id: ["OB1"] })]],
    [
      "G0079",
      [
        loading({
          "GEMs ID": "G0079",
          suspension_id: "25E32-S2",
          ocm_barcode_id: ["OB2"],
        }),
      ],
    ],
  ]);

  test("an all-OCM run has OCM plexy and wells without their own plexy", () => {
    const run = buildChromiumRun(
      [gems({}), gems({ gem_well_readable_id: "G0079" })],
      ocmRows,
      ids,
    );

    expect(run).toEqual({
      readable_id: "CR28",
      assay_id: "ocm-assay",
      run_at: "2025-05-13T00:00:00.000Z",
      run_by: "person",
      succeeded: true,
      additional_data: { ilab_request_ids: ["SCB-JAD-1294 day 4"] },
      plexy: "on_chip_multiplexing",
      gem_wells: [
        {
          readable_id: "G0078",
          loading: [{ ocm_barcode_id: "ob1", suspension_id: "s1" }],
        },
        {
          readable_id: "G0079",
          loading: [{ ocm_barcode_id: "ob2", suspension_id: "s2" }],
        },
      ],
    });
  });

  test("a run with OCM and standard wells is mixed", () => {
    const rows = new Map([
      ...ocmRows,
      ["G0091", [loading({ "GEMs ID": "G0091", suspension_id: "25E38-S5" })]],
    ]);

    const run = buildChromiumRun(
      [gems({}), gems({ gem_well_readable_id: "G0091" })],
      rows,
      ids,
    );

    expect(run.plexy).toBe("mixed");
    expect(run.gem_wells.map((w) => "plexy" in w && w.plexy)).toEqual([
      "on_chip_multiplexing",
      "standard",
    ]);
  });

  test("a clog in any well means the run failed", () => {
    const rows = new Map([
      [
        "G0078",
        [
          loading({ suspension_id: "25E32-S1", ocm_barcode_id: ["OB1"] }),
          loading({
            suspension_id: "25E32-S2",
            ocm_barcode_id: ["OB2"],
            failed: true,
          }),
        ],
      ],
    ]);

    expect(buildChromiumRun([gems({})], rows, ids).succeeded).toBe(false);
  });

  test("iLab request IDs are collected without duplicates", () => {
    const rows = new Map([
      [
        "G0078",
        [
          loading({
            suspension_id: "25E32-S1",
            ocm_barcode_id: ["OB1"],
            ilab_request_ids: ["A"],
          }),
          loading({
            suspension_id: "25E32-S2",
            ocm_barcode_id: ["OB2"],
            ilab_request_ids: ["A"],
          }),
          loading({
            suspension_id: "25E32-S6",
            ocm_barcode_id: ["OB3"],
            ilab_request_ids: ["B"],
          }),
        ],
      ],
    ]);

    expect(buildChromiumRun([gems({})], rows, ids).additional_data).toEqual({
      ilab_request_ids: ["A", "B"],
    });
  });

  test("GEMs with different assays can't be in the same run", () => {
    expect(() =>
      buildChromiumRun(
        [
          gems({}),
          gems({
            gem_well_readable_id: "G0079",
            assay_id: "Single Cell 3' Gene Expression v4",
          }),
        ],
        ocmRows,
        ids,
      ),
    ).toThrow();
  });
});

describe("assayIds", () => {
  test("matches regardless of library type order", () => {
    const assay = {
      id: "assay",
      name: "Universal 3' Gene Expression",
      chemistry_version: "v4 - GEM-X",
      chromium_chip: "GEM-X 3'",
      sample_multiplexing: "singleplex" as const,
      library_types: ["gene_expression" as const, "antibody_capture" as const],
      protocol_url: "",
    };

    expect(
      assayIds(
        {
          "Single Cell 3' Gene Expression + Cell Surface Protein v4": {
            name: "Universal 3' Gene Expression",
            library_types: ["antibody_capture", "gene_expression"],
            sample_multiplexing: "singleplex",
            chemistry_version: "v4 - GEM-X",
            chromium_chip: "GEM-X 3'",
          },
        },
        [assay],
      ),
    ).toEqual(
      new Map([
        ["Single Cell 3' Gene Expression + Cell Surface Protein v4", "assay"],
      ]),
    );
  });
});
