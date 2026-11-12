import * as z from "zod";
import { reportLocalError, writeError, type Api } from "./api.ts";
import type { RawRow } from "./export-workbook.ts";
import {
  UnknownReference,
  UnknownValue,
  type Entity,
  type Problem,
  type Subject,
} from "./report.ts";

export type Columns = Record<string, string | string[]>;

export function withCanonicalColumns(row: RawRow, columns: Columns): RawRow {
  const mapped = Object.entries(columns).map(([canonical, header]) => [
    canonical,
    typeof header === "string"
      ? row[header]
      : header.map((h) => row[h]).filter((value) => !isBlank(value)),
  ]);

  return { ...row, ...Object.fromEntries(mapped) };
}

// The workbooks use formulas that produce 0 or "" for missing values
function isBlank(value: unknown): boolean {
  return (
    value === undefined ||
    value === null ||
    value === 0 ||
    (typeof value === "string" && value.trim() === "")
  );
}

function blankTo<T>(replacement: T, transform: (value: unknown) => unknown) {
  return (value: unknown) => (isBlank(value) ? replacement : transform(value));
}

const trim = (value: unknown) => String(value).trim();
const lowercase = (value: unknown) => String(value).trim().toLowerCase();

export const EASTERN_STANDARD_TIME = "-05:00";

function toEasternStandardTime(time: string) {
  return (value: unknown): unknown => {
    if (!(value instanceof Date)) {
      return value;
    }
    const day = new Date(
      Date.UTC(value.getFullYear(), value.getMonth(), value.getDate()),
    )
      .toISOString()
      .slice(0, 10);
    return new Date(`${day}T${time}${EASTERN_STANDARD_TIME}`).toISOString();
  };
}

export const text = z.preprocess(blankTo(undefined, trim), z.string());
export const optionalText = z.preprocess(
  blankTo(null, trim),
  z.string().nullable(),
);
export const email = z.preprocess(blankTo(undefined, lowercase), z.string());
export const optionalEmail = z.preprocess(
  blankTo(null, lowercase),
  z.string().nullable(),
);
export const emails = z
  .array(email)
  .transform((addresses) => [...new Set(addresses)]);
const timestamp = z.string({ error: "should be a date" });
export const startOfWorkday = z.preprocess(
  blankTo(undefined, toEasternStandardTime("09:00")),
  timestamp,
);
export const optionalStartOfWorkday = z.preprocess(
  blankTo(null, toEasternStandardTime("09:00")),
  timestamp.nullable(),
);
export const endOfWorkday = z.preprocess(
  blankTo(undefined, toEasternStandardTime("16:00")),
  timestamp,
);
export const optionalEndOfWorkday = z.preprocess(
  blankTo(null, toEasternStandardTime("16:00")),
  timestamp.nullable(),
);
export const optionalNumber = z.preprocess(
  blankTo(null, (value) => value),
  z.number({ error: "should be a number" }).nullable(),
);
export const flag = z.preprocess(
  blankTo(false, (value) => value),
  z.boolean({ error: "should be TRUE or FALSE" }),
);

export async function parseRows<Schema extends z.ZodType>(
  api: Api,
  entity: Entity,
  schema: Schema,
  rows: RawRow[],
  identify: (row: RawRow) => string,
): Promise<z.output<Schema>[]> {
  const parsed: z.output<Schema>[] = [];

  for (const row of rows) {
    const result = schema.safeParse(row);
    if (result.success) {
      parsed.push(result.data);
    } else {
      await reportInvalidRow(
        api,
        { entity, id: identify(row) },
        row,
        result.error,
      );
    }
  }

  return parsed;
}

async function reportInvalidRow(
  api: Api,
  subject: Subject,
  row: RawRow,
  error: z.ZodError,
): Promise<void> {
  const problems = error.issues.map((issue): Problem => {
    const field = String(issue.path[0]);
    const value = row[field];
    return isBlank(value)
      ? { type: "missing_value", subject, field }
      : {
          type: "invalid_value",
          subject,
          field,
          value: String(value),
          message: issue.message,
        };
  });
  api.problems.push(...problems);

  await writeError(api, subject, {
    request: row,
    response: { error: z.prettifyError(error) },
  });
}

export async function buildEach<Item, Body>(
  api: Api,
  entity: Entity,
  items: Item[],
  identify: (item: Item) => string,
  build: (item: Item) => Body,
): Promise<Body[]> {
  const bodies: Body[] = [];

  for (const item of items) {
    try {
      bodies.push(build(item));
    } catch (error) {
      await reportLocalError(api, { entity, id: identify(item) }, item, error);
    }
  }

  return bodies;
}

// Unlike `Omit`, this distributes over the unions in the API's types
export type WithoutMeasurements<T> = T extends unknown
  ? Omit<T, "measurements">
  : never;

// A record whose measurements can't be built is still synced, without them
export async function buildEachWithMeasurements<Item, Body, Measurement>(
  api: Api,
  entities: { record: Entity; measurements: Entity },
  items: Item[],
  identify: (item: Item) => string,
  build: (item: Item) => Body,
  buildMeasurements: (item: Item, body: Body) => Measurement[],
): Promise<(Body & { measurements: Measurement[] })[]> {
  const bodies: (Body & { measurements: Measurement[] })[] = [];

  for (const item of items) {
    const id = identify(item);

    let body: Body;
    try {
      body = build(item);
    } catch (error) {
      await reportLocalError(api, { entity: entities.record, id }, item, error);
      continue;
    }

    let measurements: Measurement[] = [];
    try {
      measurements = buildMeasurements(item, body);
    } catch (error) {
      await reportLocalError(
        api,
        { entity: entities.measurements, id },
        item,
        error,
      );
    }

    bodies.push({ ...body, measurements });
  }

  return bodies;
}

export function lookup<T>(
  ids: ReadonlyMap<string, T>,
  key: string,
  entity: Entity,
): T {
  const value = ids.get(key);
  if (value === undefined) {
    throw new UnknownReference({ entity, id: key });
  }
  return value;
}

export function translate<T>(
  table: Readonly<Record<string, T>>,
  key: string,
  field: string,
): T {
  const value = table[key];
  if (value === undefined) {
    throw new UnknownValue(field, key, Object.keys(table));
  }
  return value;
}

export function notes(value: string | null): { notes: string } | null {
  return value === null ? null : { notes: value };
}
