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
  update: Schemas["PersonUpdate"];
};

export function buildPerson(
  row: PersonRow,
  institutionIds: Map<string, string>,
  oids: Map<string, string>,
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
    microsoftEntraOid: oids.get(row.email) ?? null,
    person: {
      ...fields,
      account: { email: row.email },
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

  const existingById = new Map(existing.map((p) => [p.id, p]));
  const existingByEmail = new Map(
    existing.flatMap((p) => (p.email ? [[p.email.toLowerCase(), p]] : [])),
  );
  const microsoftAccounts = accounts.filter(
    (a) => a.auth_provider === "microsoft",
  );
  const personIdByOid = new Map(
    microsoftAccounts.map((a) => [a.auth_provider_user_id.toLowerCase(), a.id]),
  );
  const personIdsWithMicrosoftAccount = new Set(
    microsoftAccounts.map((a) => a.id),
  );

  const ids = new Map<string, string>();

  for (const { email, microsoftEntraOid, person, update } of desired) {
    const oidPersonId =
      microsoftEntraOid && personIdByOid.get(microsoftEntraOid.toLowerCase());
    const current =
      (oidPersonId && existingById.get(oidPersonId)) ||
      existingByEmail.get(email);

    const id = current
      ? await updatePerson(api, email, update, microsoftEntraOid, current)
      : (
          await send(
            api,
            email,
            person,
            api.client.POST("/people", { body: person }),
          )
        )?.id;

    if (!id) {
      continue;
    }
    ids.set(email, id);

    if (microsoftEntraOid && !personIdsWithMicrosoftAccount.has(id)) {
      const account: Schemas["NewAccount"] = {
        auth_provider: "microsoft",
        microsoft_entra_oid: microsoftEntraOid,
        person_id: id,
      };
      await send(
        api,
        email,
        account,
        api.client.POST("/accounts", { body: account }),
      );
    }
  }

  return ids;
}

async function updatePerson(
  api: Api,
  email: string,
  update: Schemas["PersonUpdate"],
  microsoftEntraOid: string | null,
  current: Schemas["Person"],
): Promise<string> {
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

  return current.id;
}
