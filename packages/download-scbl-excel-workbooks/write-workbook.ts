import type { SheetSpecification } from "./config.ts";
import * as xlsx from "xlsx";

export async function exportWorkbook(
  rawFile: ReadableStream,
  sheetSpecifications: SheetSpecification[],
  outputDir: string,
) {
  const workbook = xlsx.read(await rawFile.bytes(), {
    dense: true,
    cellDates: true,
  });

  const exports = sheetSpecifications.map((spec) =>
    exportToJson(workbook, spec, outputDir)
  );

  await Promise.all(exports);
}

async function exportToJson(
  workbook: xlsx.WorkBook,
  { name, header, include_row_fn }: SheetSpecification,
  outputDir: string,
) {
  const sheet = workbook.Sheets[name];

  if (!sheet) {
    throw new Error(
      `Sheet '${name}'' does not exist. The valid sheet names for this workbook are ${workbook.SheetNames}`,
    );
  }

  let converted: Record<string, unknown>[] = xlsx.utils.sheet_to_json(sheet, {
    range: header,
  });

  if (include_row_fn) {
    converted = converted.filter((r) => include_row_fn(r));
  }

  const outputPath = `${outputDir}/${name}.json`;

  await Bun.file(outputPath).write(JSON.stringify(converted));
}
