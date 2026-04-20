import type { SheetSpecification } from "./config.ts";
import * as xlsx from "xlsx";

export async function exportWorkbook(
  rawFile: ReadableStream,
  sheetSpecifications: SheetSpecification[],
  outputDir: string,
) {
  const workbook = xlsx.read(await rawFile.bytes(), { dense: true });

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

  const outputPath = `${outputDir}/${name}.json`;

  let converted: Record<string, unknown>[] = xlsx.utils.sheet_to_json(sheet, {
    range: header,
  });

  if (include_row_fn) {
    converted = converted.filter((r) => include_row_fn(r));
  }

  const outputFile = Bun.file(outputPath);

  await outputFile.write(JSON.stringify(converted));
}
