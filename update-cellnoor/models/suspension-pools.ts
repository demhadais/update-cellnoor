import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches, unique } from "../compare.ts";
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
import { buildQuantities, toContent } from "./suspensions.ts";

const SuspensionPoolRow = z.object({
  readable_id: text,
  name: text,
  multiplexing_tag_type: optionalText,
  pooled_at: date,
  preparers: emails,
  notes: optionalText,
  pre_storage_concentration: optionalNumber,
  pre_storage_volume: optionalNumber,
  concentration: optionalNumber,
  volume: optionalNumber,
  viability: optionalNumber,
  mean_diameter: optionalNumber,
});

const PooledSuspensionRow = z.object({
  "Pooled Into ID": text,
  readable_id: text,
  content: text,
  multiplexing_tag_id: optionalText,
});

export type SuspensionPoolRow = z.infer<typeof SuspensionPoolRow>;
export type PooledSuspensionRow = z.infer<typeof PooledSuspensionRow>;

type NewSuspensionPool = Schemas["NewSuspensionPool"];

const MULTIPLEXING_TAG_TYPES: Record<string, Schemas["MultiplexingTagType"]> = {
  "Flex Barcode": "flex_barcode",
  "Flex Oligo": "flex_oligonucleotide_barcode",
  "TotalSeq-B (HTO)": "TotalSeq-B",
};

export function buildPool(
  row: SuspensionPoolRow,
  members: PooledSuspensionRow[],
  suspensionIds: Map<string, string>,
): Schemas["PooledSuspensions"] {
  const suspensionId = (member: PooledSuspensionRow) =>
    lookup(suspensionIds, member.readable_id, "suspension");

  if (!row.multiplexing_tag_type) {
    return { suspensions: members.map(suspensionId) };
  }

  return {
    multiplexing_tag_type: translate(
      MULTIPLEXING_TAG_TYPES,
      row.multiplexing_tag_type,
      "multiplexing tag type",
    ),
    suspensions: members.map((member) => ({
      suspension_id: suspensionId(member),
      tag_id: member.multiplexing_tag_id as string,
    })),
  };
}

export function buildPoolMeasurements(
  row: SuspensionPoolRow,
  members: PooledSuspensionRow[],
  personIds: Map<string, string>,
): Schemas["NewSuspensionPoolMeasurement"][] {
  const [firstMember] = members;
  if (!firstMember) {
    throw new Error("pool has no suspensions");
  }
  const content = toContent(firstMember.content);

  const values = [
    {
      concentration: row.pre_storage_concentration,
      volume: row.pre_storage_volume,
    },
    {
      concentration: row.concentration,
      volume: row.volume,
      viability: row.viability,
      mean_diameter: row.mean_diameter,
    },
  ];

  const measurements = values.flatMap((v) =>
    buildQuantities(v, content, null).map((data) => ({
      measured_by: lookup(personIds, row.preparers[0] ?? "", "person"),
      measured_at: row.pooled_at,
      data,
    })),
  );

  return unique(measurements);
}

export function buildSuspensionPool(
  row: SuspensionPoolRow,
  members: PooledSuspensionRow[],
  ids: { people: Map<string, string>; suspensions: Map<string, string> },
): WithoutMeasurements<NewSuspensionPool> {
  return {
    readable_id: row.readable_id,
    name: row.name,
    pooled_at: row.pooled_at,
    preparers: row.preparers.map((e) => lookup(ids.people, e, "person")),
    additional_data: notes(row.notes),
    pool: buildPool(row, members, ids.suspensions),
  };
}

// Returns a map from readable ID to ID
export async function syncSuspensionPools(
  api: Api,
  rawRows: { pools: RawRow[]; suspensions: RawRow[] },
  ids: { people: Map<string, string>; suspensions: Map<string, string> },
): Promise<Map<string, string>> {
  const rows = await parseRows(api, SuspensionPoolRow, rawRows.pools, (row) =>
    String(row.readable_id),
  );
  // Invalid suspension rows are reported when syncing suspensions
  const pooledSuspensions = rawRows.suspensions.flatMap((row) => {
    const result = PooledSuspensionRow.safeParse(row);
    return result.success ? [result.data] : [];
  });
  const membersByPool = Map.groupBy(
    pooledSuspensions,
    (row) => row["Pooled Into ID"],
  );

  const pools = await buildEachWithMeasurements(
    api,
    rows,
    (row) => row.readable_id,
    (row) =>
      buildSuspensionPool(row, membersByPool.get(row.readable_id) ?? [], ids),
    (row) =>
      buildPoolMeasurements(
        row,
        membersByPool.get(row.readable_id) ?? [],
        ids.people,
      ),
  );

  const existing = await fetchOrThrow(
    api.client.GET("/suspension-pools/detailed"),
  );
  const existingByReadableId = new Map(existing.map((p) => [p.readable_id, p]));

  const poolIds = new Map(existing.map((p) => [p.readable_id, p.id]));

  for (const pool of pools) {
    const current = existingByReadableId.get(pool.readable_id);

    if (!current) {
      const created = await send(
        api,
        pool.readable_id,
        pool,
        api.client.POST("/suspension-pools", { body: pool }),
      );
      if (created) {
        poolIds.set(created.readable_id, created.id);
      }
      continue;
    }

    await updateSuspensionPool(api, pool, current);
  }

  return poolIds;
}

async function updateSuspensionPool(
  api: Api,
  pool: NewSuspensionPool,
  current: Schemas["SuspensionPoolDetailed"],
): Promise<void> {
  const { measurements, preparers, pool: _, ...record } = pool;
  const params = { path: { id: current.id } };

  if (!matches(record, current)) {
    await send(
      api,
      pool.readable_id,
      record,
      api.client.PUT("/suspension-pools/{id}", { params, body: record }),
    );
  }

  await syncMeasurements(
    api,
    pool.readable_id,
    measurements,
    current.measurements,
    {
      add: (body) =>
        api.client.POST("/suspension-pools/{id}/measurements", {
          params,
          body,
        }),
      remove: (body) =>
        api.client.DELETE("/suspension-pools/{id}/measurements", {
          params,
          body,
        }),
    },
  );

  await syncIds(api, pool.readable_id, preparers, current.preparers, {
    add: (body) =>
      api.client.POST("/suspension-pools/{id}/preparers", { params, body }),
    remove: (body) =>
      api.client.DELETE("/suspension-pools/{id}/preparers", { params, body }),
  });
}
