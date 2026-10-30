import * as z from "zod";
import { reportLocalError, type Api } from "./api.ts";
import type { RawRow } from "./export-workbook.ts";

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

function toUtcDay(value: unknown): unknown {
  if (!(value instanceof Date)) {
    return value;
  }
  return new Date(
    Date.UTC(value.getFullYear(), value.getMonth(), value.getDate()),
  ).toISOString();
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
export const date = z.preprocess(blankTo(undefined, toUtcDay), z.string());
export const optionalDate = z.preprocess(
  blankTo(null, toUtcDay),
  z.string().nullable(),
);
export const optionalNumber = z.preprocess(
  blankTo(null, (value) => value),
  z.number().nullable(),
);
export const flag = z.preprocess(
  blankTo(false, (value) => value),
  z.boolean(),
);

export async function parseRows<Schema extends z.ZodType>(
  api: Api,
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
      await reportLocalError(
        api,
        identify(row),
        row,
        z.prettifyError(result.error),
      );
    }
  }

  return parsed;
}

export async function buildEach<Item, Body>(
  api: Api,
  items: Item[],
  identify: (item: Item) => string,
  build: (item: Item) => Body,
): Promise<Body[]> {
  const bodies: Body[] = [];

  for (const item of items) {
    try {
      bodies.push(build(item));
    } catch (error) {
      await reportLocalError(api, identify(item), item, error);
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
  items: Item[],
  identify: (item: Item) => string,
  build: (item: Item) => Body,
  buildMeasurements: (item: Item, body: Body) => Measurement[],
): Promise<(Body & { measurements: Measurement[] })[]> {
  const bodies: (Body & { measurements: Measurement[] })[] = [];

  for (const item of items) {
    let body: Body;
    try {
      body = build(item);
    } catch (error) {
      await reportLocalError(api, identify(item), item, error);
      continue;
    }

    let measurements: Measurement[] = [];
    try {
      measurements = buildMeasurements(item, body);
    } catch (error) {
      await reportLocalError(
        api,
        `${identify(item)} measurements`,
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
  description: string,
): T {
  return orThrow(ids.get(key), key, description);
}

export function translate<T>(
  table: Readonly<Record<string, T>>,
  key: string,
  description: string,
): T {
  return orThrow(table[key], key, description);
}

function orThrow<T>(value: T | undefined, key: string, description: string): T {
  if (value === undefined) {
    throw new Error(`unknown ${description} '${key}'`);
  }
  return value;
}

export function notes(value: string | null): { notes: string } | null {
  return value === null ? null : { notes: value };
}
