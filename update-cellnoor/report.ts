import { parse } from "path";
import type { ApiError } from "./api.ts";
import type { RawRow } from "./export-workbook.ts";
import type {
  Config,
  SheetSpecification,
  WorkbookSpecification,
} from "./config.ts";

type Workbooks = Config["workbooks"];
type Sheet = { workbook: WorkbookSpecification; sheet: SheetSpecification };
type EntityInfo = { plural: string; sheet?: (workbooks: Workbooks) => Sheet };

const ENTITIES = {
  microsoft_graph_request: { plural: "Microsoft account lookups" },
  index_set: { plural: "index sets" },
  tenx_assay: { plural: "10x assays" },
  multiplexing_tag: { plural: "multiplexing tags" },
  institution: {
    plural: "institutions",
    sheet: ({ people_and_institutions: w }) => ({
      workbook: w,
      sheet: w.sheets.institutions,
    }),
  },
  institution_email_domain: { plural: "email domains" },
  person: {
    plural: "people",
    sheet: ({ people_and_institutions: w }) => ({
      workbook: w,
      sheet: w.sheets.people,
    }),
  },
  project: {
    plural: "labs",
    sheet: ({ people_and_institutions: w }) => ({
      workbook: w,
      sheet: w.sheets.labs,
    }),
  },
  specimen: {
    plural: "specimens",
    sheet: ({ sample_inventory: w }) => ({
      workbook: w,
      sheet: w.sheets.specimens,
    }),
  },
  specimen_qc_measurement: {
    plural: "specimens",
    sheet: ({ sample_inventory: w }) => ({
      workbook: w,
      sheet: w.sheets.qc_measurements,
    }),
  },
  suspension: {
    plural: "suspensions",
    sheet: ({ chromium: w }) => ({ workbook: w, sheet: w.sheets.suspensions }),
  },
  suspension_pool: {
    plural: "multiplexed suspensions",
    sheet: ({ chromium: w }) => ({
      workbook: w,
      sheet: w.sheets.multiplexed_suspensions,
    }),
  },
  chromium_run: {
    plural: "Chromium runs",
    sheet: ({ chromium: w }) => ({ workbook: w, sheet: w.sheets.gems }),
  },
  gems: {
    plural: "GEMs",
    sheet: ({ chromium: w }) => ({ workbook: w, sheet: w.sheets.gems }),
  },
  gems_loading: {
    plural: "GEMs",
    sheet: ({ chromium: w }) => ({
      workbook: w,
      sheet: w.sheets.gems_suspensions,
    }),
  },
  cdna: {
    plural: "cDNA",
    sheet: ({ chromium: w }) => ({ workbook: w, sheet: w.sheets.cdna }),
  },
  library: {
    plural: "libraries",
    sheet: ({ chromium: w }) => ({ workbook: w, sheet: w.sheets.libraries }),
  },
  chromium_dataset: { plural: "Chromium datasets" },
  chromium_dataset_file: { plural: "Chromium dataset files" },
} satisfies Record<string, EntityInfo>;

export type Entity = keyof typeof ENTITIES;

export type Subject = { entity: Entity; id: string };

export type Problem =
  | { type: "missing_value"; subject: Subject; field: string }
  | {
      type: "invalid_value";
      subject: Subject;
      field: string;
      value: string;
      message: string;
    }
  | {
      type: "unknown_value";
      subject: Subject;
      field: string;
      value: string;
      allowed: string[];
    }
  | { type: "unknown_reference"; subject: Subject; reference: Subject }
  | { type: "other"; subject: Subject; message: string }
  | { type: "rejected"; subject: Subject; body: ApiError | string | undefined }
  | { type: "parent_failed"; subject: Subject; parent: Subject };

export class MissingValue extends Error {
  constructor(readonly field: string) {
    super(`missing ${field}`);
  }
}

export class UnknownValue extends Error {
  constructor(
    readonly field: string,
    readonly value: string,
    readonly allowed: string[],
  ) {
    super(`unknown ${field} '${value}'`);
  }
}

export class UnknownReference extends Error {
  constructor(readonly reference: Subject) {
    super(`unknown ${reference.entity} '${reference.id}'`);
  }
}

export function problemFromError(subject: Subject, error: unknown): Problem {
  if (error instanceof MissingValue) {
    return { type: "missing_value", subject, field: error.field };
  }
  if (error instanceof UnknownValue) {
    const { field, value, allowed } = error;
    return { type: "unknown_value", subject, field, value, allowed };
  }
  if (error instanceof UnknownReference) {
    return { type: "unknown_reference", subject, reference: error.reference };
  }
  return {
    type: "other",
    subject,
    message: error instanceof Error ? error.message : String(error),
  };
}

export function responsiblePeople(chromium: {
  suspensions: RawRow[];
  multiplexed_suspensions: RawRow[];
  gems: RawRow[];
  cdna: RawRow[];
  libraries: RawRow[];
}): Map<string, string> {
  const people = new Map<string, string>();

  const add = (entity: Entity, id: unknown, role: string, emails: unknown) => {
    const names = [emails]
      .flat()
      .filter((email) => typeof email === "string" && email.trim() !== "")
      .map((email) => firstName(String(email)));
    if (names.length > 0) {
      people.set(
        key({ entity, id: String(id).trim() }),
        `${role} ${names.join(" and ")}`,
      );
    }
  };

  for (const row of chromium.suspensions) {
    add("suspension", row.readable_id, "prepared by", row.preparers);
  }
  for (const row of chromium.multiplexed_suspensions) {
    add("suspension_pool", row.readable_id, "prepared by", row.preparers);
  }
  for (const row of chromium.cdna) {
    add("cdna", row.readable_id, "prepared by", row.preparers);
  }
  for (const row of chromium.libraries) {
    add("library", row.readable_id, "prepared by", row.preparers);
  }
  for (const row of chromium.gems) {
    add("chromium_run", row.readable_id, "run by", row.run_by);
    add("gems", row.gem_well_readable_id, "run by", row.run_by);
    add("gems_loading", row.gem_well_readable_id, "run by", row.run_by);
  }

  return people;
}

// Emails are assumed to be of the form first.last@domain
function firstName(email: string): string {
  const [first = ""] = email.trim().split(/[.@]/);
  return first.charAt(0).toUpperCase() + first.slice(1);
}

export function renderReport(
  problems: Problem[],
  workbooks: Workbooks,
  people: Map<string, string>,
): string {
  const failed = new Set(problems.map(({ subject }) => key(subject)));
  const categories = new Map<string, Entry & { items: string[] }>();
  let unfixable = 0;

  for (const problem of problems) {
    if (
      problem.type === "parent_failed" ||
      (problem.type === "unknown_reference" &&
        failed.has(key(problem.reference)))
    ) {
      continue;
    }

    const entry = describe(problem, workbooks, people);
    if (!entry) {
      unfixable++;
      continue;
    }

    const category = categories.get(entry.header) ?? { ...entry, items: [] };
    category.items.push(entry.item);
    categories.set(entry.header, category);
  }

  const sections = [...categories.values()].map(({ header, items, footer }) =>
    [header, ...items, footer].join("\n"),
  );
  if (unfixable > 0) {
    sections.push(
      `${unfixable} other ${unfixable === 1 ? "problem" : "problems"} can't be fixed by editing the workbooks.`,
    );
  }

  return sections.length > 0
    ? `${sections.join("\n\n")}\n`
    : "No problems were found.\n";
}

function key({ entity, id }: Subject): string {
  return JSON.stringify([entity, id]);
}

type Entry = { header: string; item: string; footer: string };

function describe(
  problem: Exclude<Problem, { type: "parent_failed" }>,
  workbooks: Workbooks,
  people: Map<string, string>,
): Entry | undefined {
  const { entity, id } = problem.subject;
  const { plural, sheet } = locate(entity, workbooks);
  const responsible = people.get(key(problem.subject));
  const label = responsible ? `${id}, ${responsible}` : id;

  if (!sheet && problem.type !== "unknown_reference") {
    return undefined;
  }

  const these = sheet
    ? `In the ${where(sheet)}, the following ${plural}`
    : `The following ${plural}`;

  switch (problem.type) {
    case "missing_value": {
      const column = columnName(sheet, problem.field);
      return {
        header: `${these} have nothing in the ${column} column:`,
        item: `- ${label}`,
        footer: `Please fill in ${column} for these ${plural}.`,
      };
    }
    case "invalid_value": {
      const column = columnName(sheet, problem.field);
      return {
        header: `${these} have something in the ${column} column that isn't valid:`,
        item: `- ${label}: "${problem.value}" ${problem.message}`,
        footer: `Please correct ${column} for these ${plural}.`,
      };
    }
    case "unknown_value": {
      const column = columnName(sheet, problem.field);
      const allowed = problem.allowed.map((value) => `"${value}"`).join(", ");
      return {
        header: `${these} have something in the ${column} column that isn't recognized:`,
        item: `- ${label}: "${problem.value}"`,
        footer: `Please use one of the following in ${column}: ${allowed}.`,
      };
    }
    case "unknown_reference": {
      const target = locate(problem.reference.entity, workbooks);
      return {
        header: `${these} refer to ${target.plural} that could not be found:`,
        item: `- ${label}: ${problem.reference.id || "(blank)"}`,
        footer: target.sheet
          ? `Please correct these, or add the missing ${target.plural} to the ${where(target.sheet)}.`
          : "Please correct these.",
      };
    }
    case "other":
      return {
        header: `${these} could not be processed:`,
        item: `- ${label}: ${problem.message}`,
        footer: `Please check these ${plural}.`,
      };
    case "rejected": {
      const { body } = problem;
      if (typeof body !== "object" || !("type" in body)) {
        return undefined;
      }

      switch (body.type) {
        case "data_constraint":
          return {
            header: `${these} could not be saved:`,
            item: body.detail
              ? `- ${label}: ${body.message}\n  ${body.detail}`
              : `- ${label}: ${body.message}`,
            footer: `Please check these ${plural} using the messages above.`,
          };
        case "duplicate_resource":
          return {
            header: body.field
              ? `${these} have the same ${columnName(sheet, body.field)} as another one:`
              : `${these} are duplicates of others:`,
            item: `- ${label}`,
            footer: `Please make sure each of these ${plural} is unique.`,
          };
        default:
          return undefined;
      }
    }
  }
}

function locate(
  entity: Entity,
  workbooks: Workbooks,
): { plural: string; sheet?: Sheet } {
  const info: EntityInfo = ENTITIES[entity];
  return { plural: info.plural, sheet: info.sheet?.(workbooks) };
}

function where({ workbook, sheet }: Sheet): string {
  return `"${sheet.name}" sheet of the "${parse(workbook.file_path).name}" workbook`;
}

function columnName(sheet: Sheet | undefined, field: string): string {
  const header = sheet?.sheet.columns[field] ?? field;
  return [header]
    .flat()
    .map((h) => `"${h}"`)
    .join(" or ");
}
