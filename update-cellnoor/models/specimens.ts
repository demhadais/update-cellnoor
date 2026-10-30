import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches, unique } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import {
  buildEachWithMeasurements,
  date,
  email,
  lookup,
  notes,
  optionalDate,
  optionalEmail,
  optionalNumber,
  optionalText,
  parseRows,
  text,
  translate,
  type WithoutMeasurements,
} from "../rows.ts";
import { syncMeasurements } from "../sync.ts";

const SpecimenRow = z.object({
  readable_id: text,
  name: text,
  submitted_by: email,
  project_id: text,
  received_at: date,
  returned_at: optionalDate,
  returned_by: optionalEmail,
  type: text,
  preservation_method: optionalText,
  embedded_in: optionalText,
  species: text,
  tissue: text,
  notes: optionalText,
});

const QcMeasurementRow = z.object({
  "Specimen ID": text,
  rin: optionalNumber,
  dv200: optionalNumber,
  instrument_name: optionalText,
  measured_by: optionalEmail,
  measured_at: optionalDate,
});

export type SpecimenRow = z.infer<typeof SpecimenRow>;
export type QcMeasurementRow = z.infer<typeof QcMeasurementRow>;

type NewSpecimen = Schemas["NewSpecimen"];
type SpecimenTypeFields =
  | ({ type: "block" } & Schemas["BlockFields"])
  | {
      type: "cell_pellet";
      thermal_preservation_method: Schemas["FlashFreezing"];
    }
  | { type: "rna_extract" }
  | ({ type: "suspension" } & Schemas["SuspensionSpecimenFields"])
  | ({ type: "tissue" } & Schemas["TissueFields"]);

type Preservation = {
  preservation_state: "fresh" | "fixed" | "thermally_preserved";
  fixative?: Schemas["Fixative"];
  thermal_preservation_method?: Schemas["ThermalPreservationMethod"];
};

const SPECIMEN_TYPES: Record<string, Schemas["SpecimenType"]> = {
  Block: "block",
  Curl: "block",
  "Cell Pellet": "cell_pellet",
  "Cell Suspension": "suspension",
  "Nucleus Suspension": "suspension",
  "RNA Extract": "rna_extract",
  "RNA Extraction": "rna_extract",
  Tissue: "tissue",
};

const PRESERVATION_METHODS: Record<string, Preservation> = {
  Fresh: { preservation_state: "fresh" },
  "Formaldehyde-derivative fixed": {
    preservation_state: "fixed",
    fixative: "formaldehyde_derivative",
  },
  "DSP-fixed": {
    preservation_state: "fixed",
    fixative: "dithiobis_succinimidylpropionate",
  },
  "Scale DSP-Fixed": {
    preservation_state: "fixed",
    fixative: "dithiobis_succinimidylpropionate",
  },
  "Flash-frozen": {
    preservation_state: "thermally_preserved",
    thermal_preservation_method: "flash_freezing",
  },
  "Cryopreserved (controlled-rate freezing)": {
    preservation_state: "thermally_preserved",
    thermal_preservation_method: "controlled_rate_freezing",
  },
};

const EMBEDDING_MATRICES: Record<string, Schemas["BlockEmbeddingMatrix"]> = {
  CMC: "carboxymethyl_cellulose",
  OCT: "optimal_cutting_temperature_compound",
  Paraffin: "paraffin",
};

export function speciesFields(value: string): {
  species: Schemas["Species"];
  host_species: Schemas["Species"] | null;
} {
  // e.g. "Homo sapiens + Mus musculus (PDX)"
  const [species, hostSpecies] = value
    .replace(/\(.*\)/, "")
    .split("+")
    .map((s) => s.trim().toLowerCase().replaceAll(" ", "_"));

  return {
    species: species as Schemas["Species"],
    host_species: (hostSpecies ?? null) as Schemas["Species"] | null,
  };
}

export function typeFields(
  row: Pick<SpecimenRow, "type" | "preservation_method" | "embedded_in">,
): SpecimenTypeFields {
  const type = translate(SPECIMEN_TYPES, row.type, "specimen type");
  const preservation = row.preservation_method
    ? translate(
        PRESERVATION_METHODS,
        row.preservation_method,
        "preservation method",
      )
    : undefined;

  switch (type) {
    case "block":
      return {
        type,
        embedded_in: row.embedded_in
          ? translate(EMBEDDING_MATRICES, row.embedded_in, "embedding matrix")
          : undefined,
        fixative: preservation?.fixative,
        thermal_preservation_method: preservation?.thermal_preservation_method,
      } as SpecimenTypeFields;
    case "cell_pellet":
      return {
        type,
        thermal_preservation_method: preservation?.thermal_preservation_method,
      } as SpecimenTypeFields;
    case "rna_extract":
      return { type };
    case "suspension":
    case "tissue":
      return { type, ...preservation } as SpecimenTypeFields;
  }
}

export function buildSpecimenMeasurements(
  row: QcMeasurementRow,
  receivedAt: string,
  personIds: Map<string, string>,
): Schemas["NewSpecimenMeasurement"][] {
  const { measured_by } = row;
  if (!measured_by) {
    throw new Error(
      row.rin === null && row.dv200 === null
        ? "QC measurement has no measurer, and no RIN or DV200"
        : "QC measurement has no measurer",
    );
  }

  const quantities = [
    ["RIN", row.rin],
    ["DV200", row.dv200],
  ] as const;

  return quantities.flatMap(([quantity, value]) =>
    value === null
      ? []
      : [
          {
            measured_by: lookup(personIds, measured_by, "person"),
            measured_at: row.measured_at ?? receivedAt,
            data: { quantity, value, instrument_name: row.instrument_name },
          },
        ],
  );
}

export function buildSpecimen(
  row: SpecimenRow,
  ids: { people: Map<string, string>; projects: Map<string, string> },
): WithoutMeasurements<NewSpecimen> {
  return {
    readable_id: row.readable_id,
    name: row.name,
    submitted_by: lookup(ids.people, row.submitted_by, "person"),
    project_id: lookup(ids.projects, row.project_id, "project"),
    received_at: row.received_at,
    returned_at: row.returned_at,
    returned_by:
      row.returned_by && lookup(ids.people, row.returned_by, "person"),
    tissue: row.tissue,
    additional_data: notes(row.notes),
    ...speciesFields(row.species),
    ...typeFields(row),
  };
}

export type SpecimenReference = { id: string; received_at: string };

// Returns a map from readable ID to the ID and received date
export async function syncSpecimens(
  api: Api,
  rawRows: { specimens: RawRow[]; qcMeasurements: RawRow[] },
  ids: { people: Map<string, string>; projects: Map<string, string> },
): Promise<Map<string, SpecimenReference>> {
  const rows = await parseRows(api, SpecimenRow, rawRows.specimens, (row) =>
    String(row.readable_id),
  );
  const qcRows = await parseRows(
    api,
    QcMeasurementRow,
    rawRows.qcMeasurements,
    (row) => `${row["Specimen ID"]} QC`,
  );
  const qcRowsBySpecimen = Map.groupBy(qcRows, (row) => row["Specimen ID"]);

  const specimens = await buildEachWithMeasurements(
    api,
    rows,
    (row) => row.readable_id,
    (row) => buildSpecimen(row, ids),
    (row) =>
      unique(
        (qcRowsBySpecimen.get(row.readable_id) ?? []).flatMap((qc) =>
          buildSpecimenMeasurements(qc, row.received_at, ids.people),
        ),
      ),
  );

  const existing = await fetchOrThrow(api.client.GET("/specimens/detailed"));
  const existingByReadableId = new Map(existing.map((s) => [s.readable_id, s]));

  const references = new Map(
    existing.map((s) => [
      s.readable_id,
      { id: s.id, received_at: s.received_at },
    ]),
  );

  for (const specimen of specimens) {
    const current = existingByReadableId.get(specimen.readable_id);

    if (!current) {
      const created = await send(
        api,
        specimen.readable_id,
        specimen,
        api.client.POST("/specimens", { body: specimen }),
      );
      if (created) {
        references.set(created.readable_id, {
          id: created.id,
          received_at: created.received_at,
        });
      }
      continue;
    }

    await updateSpecimen(api, specimen, current);
  }

  return references;
}

async function updateSpecimen(
  api: Api,
  specimen: NewSpecimen,
  current: Schemas["SpecimenDetailed"],
): Promise<void> {
  const { measurements, ...record } = specimen;
  const params = { path: { id: current.id } };

  // `SpecimenDetailed` calls the type `type_` and doesn't return the
  // preservation state (it's implied by the fixative and thermal preservation
  // method)
  const comparable = {
    ...record,
    type: undefined,
    type_: record.type,
    preservation_state: undefined,
  };

  if (!matches(comparable, current)) {
    await send(
      api,
      specimen.readable_id,
      record,
      api.client.PUT("/specimens/{id}", { params, body: record }),
    );
  }

  await syncMeasurements(
    api,
    specimen.readable_id,
    measurements,
    current.measurements,
    {
      add: (body) =>
        api.client.POST("/specimens/{id}/measurements", { params, body }),
      remove: (body) =>
        api.client.DELETE("/specimens/{id}/measurements", { params, body }),
    },
  );
}
