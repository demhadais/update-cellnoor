import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import { parseRows, text } from "../rows.ts";

const InstitutionRow = z.object({
  name: text,
  microsoft_entra_tenant_id: text,
});

type InstitutionRow = z.infer<typeof InstitutionRow>;

// People's institutions are decided by their email domain. Workbook
// institutions that no domain maps to (e.g. UCHC, JAX-MG) aren't created, so
// their people end up in the institution their domain maps to.
const INSTITUTIONS_BY_EMAIL_DOMAIN: Record<string, string> = {
  "connecticutchildrens.org": "Connecticut Children’s Research Institute",
  "cshl.edu": "Cold Spring Harbor Laboratory",
  "houstonmethodist.org": "Houston Methodist",
  "mdanderson.org": "Banner MD Anderson Cancer Center",
  "nih.gov": "National Institutes of Health",
  "psu.edu": "Pennsylvania State University",
  "purdue.edu": "Purdue University",
  "uchc.edu": "University of Connecticut",
  "uconn.edu": "University of Connecticut",
  "yale.edu": "Yale University",
};

// JAX isn't created from the workbook, because the API creates it with the nil
// UUID as its ID (see cellnoor's db/migrations/0025_insert-admin.up.sql)
const JAX_EMAIL_DOMAIN = "jax.org";
const JAX_INSTITUTION_ID = "00000000-0000-0000-0000-000000000000";

export function buildInstitution(
  row: InstitutionRow,
): Schemas["NewInstitution"] {
  return {
    name: row.name,
    microsoft_entra_tenant_id: row.microsoft_entra_tenant_id,
  };
}

// Returns a map from email domain to ID
export async function syncInstitutions(
  api: Api,
  rawRows: RawRow[],
): Promise<Map<string, string>> {
  const names = new Set(Object.values(INSTITUTIONS_BY_EMAIL_DOMAIN));
  const rows = await parseRows(
    api,
    InstitutionRow,
    rawRows.filter((row) => names.has(String(row.name))),
    (row) => String(row.name),
  );

  const existing = await fetchOrThrow(api.client.GET("/institutions"));
  const existingByName = new Map(existing.map((i) => [i.name, i]));

  const idsByName = new Map<string, string>();

  for (const row of rows) {
    const body = buildInstitution(row);
    const current = existingByName.get(body.name);

    if (!current) {
      const created = await send(
        api,
        body.name,
        body,
        api.client.POST("/institutions", { body }),
      );
      if (created) {
        idsByName.set(body.name, created.id);
      }
      continue;
    }

    idsByName.set(body.name, current.id);

    if (!matches(body, current)) {
      await send(
        api,
        body.name,
        body,
        api.client.PUT("/institutions/{id}", {
          params: { path: { id: current.id } },
          body,
        }),
      );
    }
  }

  const ids = new Map([[JAX_EMAIL_DOMAIN, JAX_INSTITUTION_ID]]);
  for (const [domain, name] of Object.entries(INSTITUTIONS_BY_EMAIL_DOMAIN)) {
    const id = idsByName.get(name);
    if (id) {
      ids.set(domain, id);
    }
  }

  return ids;
}
