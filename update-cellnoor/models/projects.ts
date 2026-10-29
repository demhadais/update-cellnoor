import * as z from "zod";
import { fetchOrThrow, send, type Api, type Schemas } from "../api.ts";
import { matches } from "../compare.ts";
import type { RawRow } from "../export-workbook.ts";
import { buildEach, emails, lookup, parseRows, text } from "../rows.ts";
import { syncIds } from "../sync.ts";

const STARTED_AT = "2014-01-01T00:00:00.000Z";

const ProjectRow = z.object({
  name: text,
  members: emails,
});

type ProjectRow = z.infer<typeof ProjectRow>;

export function buildProject(
  row: ProjectRow,
  personIds: Map<string, string>,
): Schemas["NewProject"] {
  return {
    name: row.name,
    members: row.members.map((e) => lookup(personIds, e, "person")),
    started_at: STARTED_AT,
    ended_at: null,
  };
}

// Returns a map from name to ID
export async function syncProjects(
  api: Api,
  rawRows: RawRow[],
  personIds: Map<string, string>,
): Promise<Map<string, string>> {
  const rows = await parseRows(api, ProjectRow, rawRows, (row) =>
    String(row.name),
  );
  const projects = await buildEach(
    api,
    rows,
    (row) => row.name,
    (row) => buildProject(row, personIds),
  );

  const existing = await fetchOrThrow(api.client.GET("/projects/detailed"));
  const existingByName = new Map(existing.map((p) => [p.name, p]));

  const ids = new Map<string, string>();

  for (const project of projects) {
    const current = existingByName.get(project.name);

    if (!current) {
      const created = await send(
        api,
        project.name,
        project,
        api.client.POST("/projects", { body: project }),
      );
      if (created) {
        ids.set(created.name, created.id);
      }
      continue;
    }

    ids.set(current.name, current.id);

    const { members, ...record } = project;
    const params = { path: { id: current.id } };

    if (!matches(record, current)) {
      await send(
        api,
        project.name,
        record,
        api.client.PUT("/projects/{id}", { params, body: record }),
      );
    }

    await syncIds(api, project.name, members, current.members, {
      add: (ids) => api.client.POST("/projects/{id}/people", { params, body: ids }),
      remove: (ids) =>
        api.client.DELETE("/projects/{id}/people", { params, body: ids }),
    });
  }

  return ids;
}
