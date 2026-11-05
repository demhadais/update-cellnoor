import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import {
  buildEach,
  email,
  flag,
  lookup,
  optionalText,
  parseRows,
  text,
} from "../rows.ts";

const PersonRow = z.object({
  name: text,
  email: email,
  orcid: optionalText,
  is_staff: flag,
});

type PersonRow = z.infer<typeof PersonRow>;

type DesiredPerson = {
  email: string;
  microsoftEntraOid: string | null;
  person: Schemas["NewPerson"];
};

export function buildPerson(
  row: PersonRow,
  institutionIds: Map<string, string>,
  oids: Map<string, string>,
): DesiredPerson {
  const microsoftEntraOid = oids.get(row.email) ?? null;

  return {
    email: row.email,
    microsoftEntraOid,
    person: {
      name: row.name,
      institution_id: lookup(
        institutionIds,
        row.email.slice(row.email.indexOf("@") + 1),
        "institution email domain",
      ),
      is_staff: row.is_staff,
      orcid: row.orcid,
      email: row.email,
      auth_account: microsoftEntraOid
        ? { auth_provider: "microsoft", microsoft_entra_oid: microsoftEntraOid }
        : null,
      permissions_to_grant: [],
    },
  };
}

// Returns a map from (lowercase) email to ID
export async function syncPeople(
  api: Api,
  rawRows: RawRow[],
  institutionIds: Map<string, string>,
  oids: Map<string, string>,
): Promise<Map<string, string>> {
  const rows = await parseRows(api, PersonRow, rawRows, (row) =>
    String(row.email),
  );
  const desired = await buildEach(
    api,
    rows,
    (row) => row.email,
    (row) => buildPerson(row, institutionIds, oids),
  );

  const existing = await fetchOrThrow(api.client.GET("/people"));
  const accounts = await fetchOrThrow(api.client.GET("/accounts"));

  const personIdByEmail = new Map(
    existing.flatMap((p) => (p.email ? [[p.email.toLowerCase(), p.id]] : [])),
  );
  const personIdByOid = new Map(
    accounts
      .filter((a) => a.auth_provider === "microsoft")
      .map((a) => [a.auth_provider_user_id.toLowerCase(), a.person_id]),
  );

  const ids = new Map<string, string>();

  for (const { email, microsoftEntraOid, person } of desired) {
    const existingId =
      (microsoftEntraOid && personIdByOid.get(microsoftEntraOid.toLowerCase())) ||
      personIdByEmail.get(email);

    if (existingId) {
      ids.set(email, existingId);
      continue;
    }

    const created = await send(
      api,
      email,
      person,
      api.client.POST("/people", { body: person }),
    );
    if (created) {
      ids.set(email, created.id);
    }
  }

  return ids;
}
