import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches, unique } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import {
  buildEachWithMeasurements,
  emails,
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
import { syncIds, syncMeasurements } from "../sync.ts";
import type { SpecimenReference } from "./specimens.ts";

const SuspensionRow = z.object({
  readable_id: text,
  specimen_id: text,
  content: text,
  created_at: optionalDate,
  lysis_duration_minutes: optionalNumber,
  target_cell_recovery: optionalNumber,
  preparers: emails,
  notes: optionalText,
  counting_method: optionalText,
  customer_measured_by: optionalEmail,
  customer_concentration: optionalNumber,
  customer_volume: optionalNumber,
  customer_viability: optionalNumber,
  concentration: optionalNumber,
  volume: optionalNumber,
  viability: optionalNumber,
  mean_diameter: optionalNumber,
  adjusted_concentration: optionalNumber,
  adjusted_volume: optionalNumber,
  adjusted_viability: optionalNumber,
  adjusted_mean_diameter: optionalNumber,
  post_hybridization_concentration: optionalNumber,
  post_hybridization_volume: optionalNumber,
  post_hybridization_mean_diameter: optionalNumber,
});

export type SuspensionRow = z.infer<typeof SuspensionRow>;

type NewSuspension = Schemas["NewSuspension"];
type Content = Schemas["SuspensionContent"];

const COUNTING_METHODS: Record<string, Schemas["CountingMethod"]> = {
  AOPI: "acridine_orange_propidium_iodide",
  "Bright Field": "bright_field",
  "Trypan Blue": "trypan_blue",
};

export type QuantityValues = {
  concentration?: number | null;
  volume?: number | null;
  viability?: number | null;
  mean_diameter?: number | null;
};

export function toContent(value: string): Content {
  return value.toLowerCase() as Content;
}

export function buildQuantities(
  values: QuantityValues,
  content: Content,
  countingMethod: Schemas["CountingMethod"] | null,
): Schemas["SuspensionMeasurementQuantity"][] {
  const quantities: Schemas["SuspensionMeasurementQuantity"][] = [];

  if (values.concentration) {
    quantities.push({
      quantity: "concentration",
      // The API stores this as an integer, but the workbooks compute it with
      // formulas that produce values like 8140000.000000001
      value: Math.round(values.concentration),
      numerator_unit: content,
      denominator_unit: "milliliter",
      counting_method: countingMethod,
    });
  }
  if (values.volume) {
    quantities.push({
      quantity: "volume",
      value: values.volume,
      unit: "microliter",
    });
  }
  if (values.viability) {
    // The workbooks record viability as a percentage, the API as a fraction
    quantities.push({ quantity: "viability", value: values.viability / 100 });
  }
  if (values.mean_diameter) {
    quantities.push({
      quantity: "mean_diameter",
      value: values.mean_diameter,
      object: content,
      unit: "micrometer",
    });
  }

  return quantities;
}

export function buildSuspensionMeasurements(
  row: SuspensionRow,
  createdAt: string,
  personIds: Map<string, string>,
): Schemas["NewSuspensionMeasurement"][] {
  const content = toContent(row.content);
  const countingMethod = row.counting_method
    ? translate(COUNTING_METHODS, row.counting_method, "counting method")
    : null;

  const groups = [
    {
      measuredBy: row.customer_measured_by,
      postHybridization: false,
      countingMethod: null,
      values: {
        concentration: row.customer_concentration,
        volume: row.customer_volume,
        viability: row.customer_viability,
      },
    },
    {
      measuredBy: row.preparers[0],
      postHybridization: false,
      countingMethod,
      values: {
        concentration: row.concentration,
        volume: row.volume,
        viability: row.viability,
        mean_diameter: row.mean_diameter,
      },
    },
    {
      measuredBy: row.preparers[0],
      postHybridization: false,
      countingMethod,
      values: {
        concentration: row.adjusted_concentration,
        volume: row.adjusted_volume,
        viability: row.adjusted_viability,
        mean_diameter: row.adjusted_mean_diameter,
      },
    },
    {
      measuredBy: row.preparers[0],
      postHybridization: true,
      countingMethod,
      values: {
        concentration: row.post_hybridization_concentration,
        volume: row.post_hybridization_volume,
        mean_diameter: row.post_hybridization_mean_diameter,
      },
    },
  ];

  const measurements = groups.flatMap(
    ({ measuredBy, postHybridization, countingMethod, values }) =>
      buildQuantities(values, content, countingMethod).map((quantity) => ({
        measured_by: lookup(personIds, measuredBy ?? "", "person"),
        measured_at: createdAt,
        data: { ...quantity, post_hybridization: postHybridization },
      })),
  );

  return unique(measurements);
}

export function buildSuspension(
  row: SuspensionRow,
  ids: {
    people: Map<string, string>;
    specimens: Map<string, SpecimenReference>;
  },
): WithoutMeasurements<NewSuspension> {
  const specimen = lookup(ids.specimens, row.specimen_id, "specimen");

  return {
    readable_id: row.readable_id,
    specimen_id: specimen.id,
    content: toContent(row.content),
    created_at: row.created_at ?? specimen.received_at,
    lysis_duration_minutes: row.lysis_duration_minutes,
    target_cell_recovery:
      row.target_cell_recovery && Math.round(row.target_cell_recovery),
    preparers: row.preparers.map((e) => lookup(ids.people, e, "person")),
    additional_data: notes(row.notes),
  };
}

// Returns a map from readable ID to ID
export async function syncSuspensions(
  api: Api,
  rawRows: RawRow[],
  ids: {
    people: Map<string, string>;
    specimens: Map<string, SpecimenReference>;
  },
): Promise<Map<string, string>> {
  const rows = await parseRows(api, SuspensionRow, rawRows, (row) =>
    String(row.readable_id),
  );
  const suspensions = await buildEachWithMeasurements(
    api,
    rows,
    (row) => row.readable_id,
    (row) => buildSuspension(row, ids),
    (row, suspension) =>
      buildSuspensionMeasurements(row, suspension.created_at, ids.people),
  );

  const existing = await fetchOrThrow(api.client.GET("/suspensions/detailed"));
  const existingByReadableId = new Map(existing.map((s) => [s.readable_id, s]));

  const suspensionIds = new Map(existing.map((s) => [s.readable_id, s.id]));

  for (const suspension of suspensions) {
    const current = existingByReadableId.get(suspension.readable_id);

    if (!current) {
      const created = await send(
        api,
        suspension.readable_id,
        suspension,
        api.client.POST("/suspensions", { body: suspension }),
      );
      if (created) {
        suspensionIds.set(created.readable_id, created.id);
      }
      continue;
    }

    await updateSuspension(api, suspension, current);
  }

  return suspensionIds;
}

async function updateSuspension(
  api: Api,
  suspension: NewSuspension,
  current: Schemas["SuspensionDetailed"],
): Promise<void> {
  const { measurements, preparers, ...record } = suspension;
  const params = { path: { id: current.id } };

  if (!matches(record, current)) {
    await send(
      api,
      suspension.readable_id,
      record,
      api.client.PUT("/suspensions/{id}", { params, body: record }),
    );
  }

  await syncMeasurements(
    api,
    suspension.readable_id,
    measurements,
    current.measurements,
    {
      add: (body) =>
        api.client.POST("/suspensions/{id}/measurements", { params, body }),
      remove: (body) =>
        api.client.DELETE("/suspensions/{id}/measurements", { params, body }),
    },
  );

  await syncIds(api, suspension.readable_id, preparers, current.preparers, {
    add: (body) =>
      api.client.POST("/suspensions/{id}/preparers", { params, body }),
    remove: (body) =>
      api.client.DELETE("/suspensions/{id}/preparers", { params, body }),
  });
}
