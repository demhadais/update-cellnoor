import type { SheetSpecification } from "./config.ts";
import * as xlsx from "xlsx";
import { withCanonicalColumns } from "./rows.ts";

export type RawRow = Record<string, unknown>;

export async function exportWorkbook<
  Sheets extends Record<string, SheetSpecification>,
>(
  rawFile: Uint8Array,
  sheetSpecifications: Sheets,
  outputDir: string,
): Promise<{ [K in keyof Sheets]: RawRow[] }> {
  const workbook = xlsx.read(rawFile, {
    dense: true,
    cellDates: true,
  });

  const exports = Object.entries(sheetSpecifications).map(
    async ([key, spec]) =>
      [key, await exportToJson(workbook, spec, outputDir)] as const,
  );

  return Object.fromEntries(await Promise.all(exports)) as {
    [K in keyof Sheets]: RawRow[];
  };
}

async function exportToJson(
  workbook: xlsx.WorkBook,
  { name, header, exclude_row_fn, columns }: SheetSpecification,
  outputDir: string,
): Promise<RawRow[]> {
  const sheet = workbook.Sheets[name];

  if (!sheet) {
    throw new Error(
      `Sheet '${name}'' does not exist. The valid sheet names for this workbook are ${workbook.SheetNames}`,
    );
  }

  let converted: RawRow[] = xlsx.utils.sheet_to_json(sheet, {
    range: header,
  });

  if (exclude_row_fn) {
    converted = converted.filter((r) => !exclude_row_fn(r));
  }

  const outputPath = `${outputDir}/${name}.json`;
  await Bun.file(outputPath).write(JSON.stringify(converted));

  return converted.map((row) => withCanonicalColumns(row, columns));
}
