import * as z from "zod";
import {
  fetchOrThrow,
  reportLocalError,
  reportParentFailed,
  send,
  type Api,
  type Schemas,
} from "../api.ts";
import { matches, unique } from "../compare.ts";
import type { AssaySpecification } from "../config.ts";
import type { RawRow } from "../export-workbook.ts";
import {
  buildEach,
  startOfWorkday,
  email,
  flag,
  lookup,
  optionalText,
  parseRows,
  text,
} from "../rows.ts";

const GemsRow = z.object({
  readable_id: text,
  gem_well_readable_id: text,
  assay_id: text,
  run_at: startOfWorkday,
  run_by: email,
});

const GemsSuspensionRow = z.object({
  "GEMs ID": text,
  suspension_id: optionalText,
  suspension_pool_id: optionalText,
  ocm_barcode_id: z.array(text),
  failed: flag,
  ilab_request_ids: z.array(text),
});

export type GemsRow = z.infer<typeof GemsRow>;
export type GemsSuspensionRow = z.infer<typeof GemsSuspensionRow>;

type NewChromiumRun = Schemas["NewChromiumRun"];
type GemWell = Schemas["NewStandardOrOcmGemWell"];
type OcmGemWell = Extract<GemWell, { plexy: "on_chip_multiplexing" }>;
type StandardGemWell = Extract<GemWell, { plexy: "standard" }>;
type LoadedEntity = { suspension_id: string } | { suspension_pool_id: string };
type RunGemWells =
  | { plexy: "mixed"; gem_wells: GemWell[] }
  | {
      plexy: "on_chip_multiplexing";
      gem_wells: Schemas["NewOcmGemWell"][];
    }
  | { plexy: "standard"; gem_wells: Schemas["NewStandardGemWell"][] };

type Ids = {
  people: Map<string, string>;
  suspensions: Map<string, string>;
  pools: Map<string, string>;
  assays: Map<string, string>;
};

export function assayIds(
  assayMap: Record<string, AssaySpecification>,
  tenxAssays: Schemas["TenxAssay"][],
): Map<string, string> {
  return new Map(
    Object.entries(assayMap).flatMap(([sheetName, specification]) => {
      const assay = tenxAssays.find((a) =>
        matches(
          {
            ...specification,
            library_types: specification.library_types.toSorted(),
          },
          { ...a, library_types: a.library_types?.toSorted() },
        ),
      );
      return assay ? [[sheetName, assay.id]] : [];
    }),
  );
}

export function ocmBarcodeIds(
  row: GemsSuspensionRow,
): Schemas["OcmBarcodeId"][] {
  return row.ocm_barcode_id
    .flatMap((tags) => tags.split("+"))
    .map((tag) => tag.trim())
    .filter((tag) => /^OB\d$/i.test(tag))
    .map((tag) => tag.toUpperCase() as Schemas["OcmBarcodeId"]);
}

function loadedEntity(row: GemsSuspensionRow, ids: Ids): LoadedEntity {
  if (row.suspension_id) {
    return {
      suspension_id: lookup(ids.suspensions, row.suspension_id, "suspension"),
    };
  }
  if (row.suspension_pool_id) {
    return {
      suspension_pool_id: lookup(
        ids.pools,
        row.suspension_pool_id,
        "suspension_pool",
      ),
    };
  }
  throw new Error("row has neither a suspension nor a suspension pool");
}

export function buildGemWell(
  readableId: string,
  rows: GemsSuspensionRow[],
  ids: Ids,
): GemWell {
  const isOcm = rows.some((row) => ocmBarcodeIds(row).length > 0);

  if (isOcm) {
    const loading = rows.flatMap((row) => {
      const barcodeIds = ocmBarcodeIds(row);
      if (barcodeIds.length === 0) {
        throw new Error(
          `GEMs ${readableId} uses OCM, but one of its suspensions has no OCM barcode`,
        );
      }
      const entity = loadedEntity(row, ids);
      return barcodeIds.map((ocm_barcode_id) => ({
        ocm_barcode_id,
        ...entity,
      }));
    });

    return {
      plexy: "on_chip_multiplexing",
      readable_id: readableId,
      loading: unique(loading),
    };
  }

  const entities = unique(rows.map((row) => loadedEntity(row, ids)));
  const [entity] = entities;
  if (!entity || entities.length > 1) {
    throw new Error(
      `GEMs ${readableId} has no OCM barcodes, so it must be loaded with exactly one suspension or suspension pool, but it has ${entities.length}`,
    );
  }

  return { plexy: "standard", readable_id: readableId, ...entity };
}

export function runGemWells(wells: GemWell[]): RunGemWells {
  if (wells.every((w): w is OcmGemWell => w.plexy === "on_chip_multiplexing")) {
    return {
      plexy: "on_chip_multiplexing",
      gem_wells: wells.map(({ plexy, ...well }) => well),
    };
  }

  if (wells.every((w): w is StandardGemWell => w.plexy === "standard")) {
    return {
      plexy: "standard",
      gem_wells: wells.map(({ plexy, ...well }) => well),
    };
  }

  return { plexy: "mixed", gem_wells: wells };
}

export function buildChromiumRun(
  gemsRows: [GemsRow, ...GemsRow[]],
  rowsByGems: Map<string, GemsSuspensionRow[]>,
  ids: Ids,
): NewChromiumRun {
  const [first] = gemsRows;

  const assays = new Set(gemsRows.map((row) => row.assay_id));
  if (assays.size > 1) {
    throw new Error(`GEMs have different assays: ${[...assays].join(", ")}`);
  }

  const wells = gemsRows.map((row) =>
    buildGemWell(
      row.gem_well_readable_id,
      rowsByGems.get(row.gem_well_readable_id) ?? [],
      ids,
    ),
  );
  const rows = gemsRows.flatMap(
    (row) => rowsByGems.get(row.gem_well_readable_id) ?? [],
  );

  return {
    readable_id: first.readable_id,
    assay_id: lookup(ids.assays, first.assay_id, "tenx_assay"),
    run_at: first.run_at,
    run_by: lookup(ids.people, first.run_by, "person"),
    succeeded: !rows.some((row) => row.failed),
    additional_data: {
      ilab_request_ids: [
        ...new Set(rows.flatMap((row) => row.ilab_request_ids)),
      ],
    },
    ...runGemWells(wells),
  };
}

// Returns a map from GEM well readable ID to GEM well ID
export async function syncChromiumRuns(
  api: Api,
  rawRows: { gems: RawRow[]; gemsSuspensions: RawRow[] },
  ids: Omit<Ids, "assays"> & { assayMap: Record<string, AssaySpecification> },
): Promise<Map<string, string>> {
  const gemsRows = await parseRows(api, "gems", GemsRow, rawRows.gems, (row) =>
    String(row.gem_well_readable_id),
  );
  const gemsSuspensionRows = await parseRows(
    api,
    "gems_loading",
    GemsSuspensionRow,
    rawRows.gemsSuspensions,
    (row) => String(row["GEMs ID"]),
  );

  const tenxAssays = await fetchOrThrow(api.client.GET("/10x-assays"));
  const allIds = { ...ids, assays: assayIds(ids.assayMap, tenxAssays) };

  const rowsByGems = Map.groupBy(gemsSuspensionRows, (row) => row["GEMs ID"]);
  const gemsRowsByRun = Map.groupBy(gemsRows, (row) => row.readable_id);

  const runs = await buildEach(
    api,
    "chromium_run",
    [...gemsRowsByRun.values()] as [GemsRow, ...GemsRow[]][],
    ([first]) => first.readable_id,
    (rows) => buildChromiumRun(rows, rowsByGems, allIds),
  );

  const existing = await fetchOrThrow(
    api.client.GET("/chromium-runs/detailed"),
  );
  const existingByReadableId = new Map(existing.map((r) => [r.readable_id, r]));

  const gemWellIds = new Map(
    existing.flatMap((r) => r.gem_wells.map((w) => [w.readable_id, w.id])),
  );

  for (const run of runs) {
    const current = existingByReadableId.get(run.readable_id);

    if (!current) {
      const created = await send(
        api,
        { entity: "chromium_run", id: run.readable_id },
        run,
        api.client.POST("/chromium-runs", { body: run }),
      );
      created?.gem_wells.forEach((w) => gemWellIds.set(w.readable_id, w.id));
      continue;
    }

    const { gem_wells, plexy, ...record } = run;

    const currentWells = new Set(current.gem_wells.map((w) => w.readable_id));
    const missingWells = (gemsRowsByRun.get(run.readable_id) ?? [])
      .map((row) => row.gem_well_readable_id)
      .filter((id) => !currentWells.has(id));
    if (missingWells.length > 0) {
      await reportLocalError(
        api,
        { entity: "chromium_run", id: run.readable_id },
        run,
        new Error(
          `GEMs ${missingWells.join(", ")} can't be added, because cellnoor can't add GEMs to a Chromium run it already has`,
        ),
      );
    }

    if (!matches(record, current)) {
      await send(
        api,
        { entity: "chromium_run", id: run.readable_id },
        record,
        api.client.PUT("/chromium-runs/{id}", {
          params: { path: { id: current.id } },
          body: record,
        }),
      );
    }
  }

  for (const row of gemsRows) {
    if (!gemWellIds.has(row.gem_well_readable_id)) {
      reportParentFailed(
        api,
        { entity: "gems", id: row.gem_well_readable_id },
        { entity: "chromium_run", id: row.readable_id },
      );
    }
  }

  return gemWellIds;
}
