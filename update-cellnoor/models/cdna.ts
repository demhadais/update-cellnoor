import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import {
  buildEachWithMeasurements,
  date,
  emails,
  lookup,
  notes,
  optionalNumber,
  optionalText,
  parseRows,
  text,
  translate,
  type WithoutMeasurements,
} from "../rows.ts";
import { syncIds, syncMeasurements } from "../sync.ts";

const CdnaRow = z.object({
  readable_id: text,
  gem_well_id: text,
  library_type: text,
  n_amplification_cycles: optionalNumber,
  prepared_at: date,
  preparers: emails,
  notes: optionalText,
  concentration: optionalNumber,
  sizing_range_minimum: optionalNumber,
  sizing_range_maximum: optionalNumber,
});

export type CdnaRow = z.infer<typeof CdnaRow>;

type NewCdna = Schemas["NewCdna"];

const LIBRARY_TYPES: Record<string, Schemas["LibraryType"]> = {
  "Antibody Capture": "antibody_capture",
  "Chromatin Accessibility": "chromatin_accessibility",
  "CRISPR Guide Capture": "crispr_guide_capture",
  "Gene Expression": "gene_expression",
  "Gene Expression Flex": "gene_expression",
  "VDJ-T": "vdj_t",
};

export function electrophoreticMeasurement(
  values: {
    concentration: number | null;
    sizing_range_minimum: number | null;
    sizing_range_maximum: number | null;
    mean_size_bp?: number | null;
  },
  measurement: { measured_by: string; measured_at: string },
): Schemas["NewNucleicAcidMeasurement"][] {
  if (values.concentration === null) {
    return [];
  }

  return [
    {
      ...measurement,
      data: {
        type: "Electrophoretic",
        instrument_name: "TapeStation",
        concentration: {
          value: values.concentration,
          numerator_unit: "picogram",
          denominator_unit: "microliter",
        },
        sizing_range: [
          values.sizing_range_minimum,
          values.sizing_range_maximum,
        ] as [number, number],
        mean_size_bp: values.mean_size_bp ?? null,
      },
    },
  ];
}

export function buildCdnaMeasurements(
  row: CdnaRow,
  measuredBy: string,
): Schemas["NewNucleicAcidMeasurement"][] {
  return electrophoreticMeasurement(row, {
    measured_by: measuredBy,
    measured_at: row.prepared_at,
  });
}

export function buildCdna(
  row: CdnaRow,
  ids: { people: Map<string, string>; gemWells: Map<string, string> },
): WithoutMeasurements<NewCdna> {
  const libraryType = translate(LIBRARY_TYPES, row.library_type, "library type");
  const preparers = row.preparers.map((e) => lookup(ids.people, e, "person"));

  const libraryTypeFields =
    libraryType === "gene_expression"
      ? {
          library_type: libraryType,
          n_amplification_cycles: row.n_amplification_cycles as number,
        }
      : { library_type: libraryType };

  return {
    readable_id: row.readable_id,
    gem_well_id: lookup(ids.gemWells, row.gem_well_id, "GEMs"),
    prepared_at: row.prepared_at,
    preparers,
    additional_data: notes(row.notes),
    ...libraryTypeFields,
  };
}

// Returns a map from readable ID to ID
export async function syncCdna(
  api: Api,
  rawRows: RawRow[],
  ids: { people: Map<string, string>; gemWells: Map<string, string> },
): Promise<Map<string, string>> {
  const rows = await parseRows(api, CdnaRow, rawRows, (row) =>
    String(row.readable_id),
  );
  const cdnas = await buildEachWithMeasurements(
    api,
    rows,
    (row) => row.readable_id,
    (row) => buildCdna(row, ids),
    (row, cdna) => buildCdnaMeasurements(row, cdna.preparers[0] ?? ""),
  );

  const existing = await fetchOrThrow(
    api.client.GET("/cdna/chromium/detailed"),
  );
  const existingByReadableId = new Map(existing.map((c) => [c.readable_id, c]));

  const cdnaIds = new Map(existing.map((c) => [c.readable_id, c.id]));

  for (const cdna of cdnas) {
    const current = existingByReadableId.get(cdna.readable_id);

    if (!current) {
      const created = await send(
        api,
        cdna.readable_id,
        cdna,
        api.client.POST("/cdna", { body: cdna }),
      );
      if (created) {
        cdnaIds.set(created.readable_id, created.id);
      }
      continue;
    }

    await updateCdna(api, cdna, current);
  }

  return cdnaIds;
}

async function updateCdna(
  api: Api,
  cdna: NewCdna,
  current: { id: string; measurements: { id: string }[]; preparers: string[] },
): Promise<void> {
  const params = { path: { id: current.id } };
  const record: Schemas["CdnaSimpleFields"] = {
    readable_id: cdna.readable_id,
    prepared_at: cdna.prepared_at,
    additional_data: cdna.additional_data,
  };

  if (!matches(record, current)) {
    await send(
      api,
      cdna.readable_id,
      record,
      api.client.PUT("/cdna/{id}", { params, body: record }),
    );
  }

  await syncMeasurements(
    api,
    cdna.readable_id,
    cdna.measurements,
    current.measurements,
    {
      add: (body) => api.client.POST("/cdna/{id}/measurements", { params, body }),
      remove: (body) =>
        api.client.DELETE("/cdna/{id}/measurements", { params, body }),
    },
  );

  await syncIds(api, cdna.readable_id, cdna.preparers, current.preparers, {
    add: (body) => api.client.POST("/cdna/{id}/preparers", { params, body }),
    remove: (body) =>
      api.client.DELETE("/cdna/{id}/preparers", { params, body }),
  });
}
