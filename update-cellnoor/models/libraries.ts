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
  type WithoutMeasurements,
} from "../rows.ts";
import { syncIds, syncMeasurements } from "../sync.ts";
import { electrophoreticMeasurement } from "./cdna.ts";

const LibraryRow = z.object({
  readable_id: text,
  cdna_id: text,
  index_set_name: optionalText,
  number_of_sample_index_pcr_cycles: optionalNumber,
  target_reads_per_cell: optionalNumber,
  prepared_at: date,
  preparers: emails,
  notes: optionalText,
  concentration: optionalNumber,
  mean_size_bp: optionalNumber,
  sizing_range_minimum: optionalNumber,
  sizing_range_maximum: optionalNumber,
  qubit_concentration: optionalNumber,
});

export type LibraryRow = z.infer<typeof LibraryRow>;

type NewLibrary = Schemas["NewLibrary"];

export function indexSetFields(
  name: string | null,
  singleIndexSetNames: Set<string>,
): Pick<NewLibrary, "single_index_set_name" | "dual_index_set_name"> {
  const isSingle = name !== null && singleIndexSetNames.has(name);
  return {
    single_index_set_name: isSingle ? name : null,
    dual_index_set_name: isSingle ? null : name,
  };
}

export function buildLibraryMeasurements(
  row: LibraryRow,
  measuredBy: string,
): Schemas["NewNucleicAcidMeasurement"][] {
  const measurement = { measured_by: measuredBy, measured_at: row.prepared_at };

  const fluorometric: Schemas["NewNucleicAcidMeasurement"][] =
    row.qubit_concentration === null
      ? []
      : [
          {
            ...measurement,
            data: {
              type: "Fluorometric",
              instrument_name: "Qubit",
              concentration: {
                value: row.qubit_concentration,
                numerator_unit: "nanogram",
                denominator_unit: "microliter",
              },
            },
          },
        ];

  return [...electrophoreticMeasurement(row, measurement), ...fluorometric];
}

export function buildLibrary(
  row: LibraryRow,
  ids: {
    people: Map<string, string>;
    cdna: Map<string, string>;
    singleIndexSetNames: Set<string>;
  },
): WithoutMeasurements<NewLibrary> {
  const preparers = row.preparers.map((e) => lookup(ids.people, e, "person"));

  return {
    readable_id: row.readable_id,
    cdna_id: lookup(ids.cdna, row.cdna_id, "cDNA"),
    number_of_sample_index_pcr_cycles:
      row.number_of_sample_index_pcr_cycles as number,
    target_reads_per_cell:
      row.target_reads_per_cell && Math.round(row.target_reads_per_cell * 1000),
    prepared_at: row.prepared_at,
    preparers,
    additional_data: notes(row.notes),
    ...indexSetFields(row.index_set_name, ids.singleIndexSetNames),
  };
}

// Returns a map from readable ID to ID
export async function syncLibraries(
  api: Api,
  rawRows: RawRow[],
  ids: {
    people: Map<string, string>;
    cdna: Map<string, string>;
    singleIndexSetNames: Set<string>;
  },
): Promise<Map<string, string>> {
  const rows = await parseRows(api, LibraryRow, rawRows, (row) =>
    String(row.readable_id),
  );
  const libraries = await buildEachWithMeasurements(
    api,
    rows,
    (row) => row.readable_id,
    (row) => buildLibrary(row, ids),
    (row, library) => buildLibraryMeasurements(row, library.preparers[0] ?? ""),
  );

  const existing = await fetchOrThrow(api.client.GET("/libraries/detailed"));
  const existingByReadableId = new Map(existing.map((l) => [l.readable_id, l]));

  const libraryIds = new Map(existing.map((l) => [l.readable_id, l.id]));

  for (const library of libraries) {
    const current = existingByReadableId.get(library.readable_id);

    if (!current) {
      const created = await send(
        api,
        library.readable_id,
        library,
        api.client.POST("/libraries", { body: library }),
      );
      if (created) {
        libraryIds.set(created.readable_id, created.id);
      }
      continue;
    }

    await updateLibrary(api, library, current);
  }

  return libraryIds;
}

async function updateLibrary(
  api: Api,
  library: NewLibrary,
  current: { id: string; measurements: { id: string }[]; preparers: string[] },
): Promise<void> {
  const { measurements, preparers, ...record } = library;
  const params = { path: { id: current.id } };

  if (!matches(record, current)) {
    await send(
      api,
      library.readable_id,
      record,
      api.client.PUT("/libraries/{id}", { params, body: record }),
    );
  }

  await syncMeasurements(
    api,
    library.readable_id,
    measurements,
    current.measurements,
    {
      add: (body) =>
        api.client.POST("/libraries/{id}/measurements", { params, body }),
      remove: (body) =>
        api.client.DELETE("/libraries/{id}/measurements", { params, body }),
    },
  );

  await syncIds(api, library.readable_id, preparers, current.preparers, {
    add: (body) =>
      api.client.POST("/libraries/{id}/preparers", { params, body }),
    remove: (body) =>
      api.client.DELETE("/libraries/{id}/preparers", { params, body }),
  });
}
