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
  microsoft_entra_oid: optionalText,
});

type PersonRow = z.infer<typeof PersonRow>;

type DesiredPerson = {
  email: string;
  microsoftEntraOid: string | null;
  person: Schemas["NewPerson"];
  update: Schemas["PersonUpdate"];
};

export function buildPerson(
  row: PersonRow,
  institutionIds: Map<string, string>,
): DesiredPerson {
  const fields = {
    name: row.name,
    institution_id: lookup(
      institutionIds,
      row.email.slice(row.email.indexOf("@") + 1),
      "institution email domain",
    ),
    is_staff: row.is_staff,
    orcid: row.orcid,
  };

  return {
    email: row.email,
    microsoftEntraOid: row.microsoft_entra_oid,
    person: {
      ...fields,
      account: row.microsoft_entra_oid
        ? {
            auth_provider: "microsoft",
            microsoft_entra_oid: row.microsoft_entra_oid,
          }
        : { email: row.email },
      permissions_to_grant: [],
    },
    update: { ...fields, email: row.email },
  };
}

// Returns a map from (lowercase) email to ID
export async function syncPeople(
  api: Api,
  rawRows: RawRow[],
  institutionIds: Map<string, string>,
): Promise<Map<string, string>> {
  const rows = await parseRows(api, PersonRow, rawRows, (row) =>
    String(row.email),
  );
  const desired = await buildEach(
    api,
    rows,
    (row) => row.email,
    (row) => buildPerson(row, institutionIds),
  );

  const existing = await fetchOrThrow(api.client.GET("/people"));
  const accounts = await fetchOrThrow(api.client.GET("/accounts"));

  const existingById = new Map(existing.map((p) => [p.id, p]));
  const existingByEmail = new Map(
    existing.flatMap((p) => (p.email ? [[p.email.toLowerCase(), p]] : [])),
  );
  const personIdByOid = new Map(
    accounts
      .filter((a) => a.auth_provider === "microsoft")
      .map((a) => [a.auth_provider_user_id.toLowerCase(), a.id]),
  );

  const ids = new Map<string, string>();

  for (const { email, microsoftEntraOid, person, update } of desired) {
    const oidPersonId =
      microsoftEntraOid && personIdByOid.get(microsoftEntraOid.toLowerCase());
    const current =
      (oidPersonId && existingById.get(oidPersonId)) ||
      existingByEmail.get(email);

    if (!current) {
      const created = await send(
        api,
        email,
        person,
        api.client.POST("/people", { body: person }),
      );
      if (created) {
        ids.set(email, created.id);
      }
      continue;
    }

    ids.set(email, current.id);

    // The web app sets the email of people with a Microsoft account when they
    // sign in
    const body = microsoftEntraOid ? { ...update, email: current.email } : update;

    if (!matches(body, current)) {
      await send(
        api,
        email,
        body,
        api.client.PUT("/people/{id}", {
          params: { path: { id: current.id } },
          body,
        }),
      );
    }
  }

  return ids;
}
