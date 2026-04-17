import type { SheetSpecification, WorkbookSpecification } from "./config";
import * as xlsx from "xlsx";

export async function exportWorkbook(
  rawFile: ReadableStream,
  sheetSpecifications: SheetSpecification[],
  outputDir: string,
) {
  const workbook = xlsx.read(await rawFile.bytes());

  const exports = sheetSpecifications.map((spec) =>
    exportToJson(workbook, spec, outputDir),
  );

  await Promise.all(exports);
}

async function exportToJson(
  workbook: xlsx.WorkBook,
  { name, include_row_fn }: SheetSpecification,
  outputDir: string,
) {
  const sheet = workbook.Sheets[name];

  if (!sheet) {
    throw new Error(
      `Sheet '${name}'' does not exist. The valid sheet names for this workbook are ${workbook.SheetNames}`,
    );
  }

  const outputPath = `${outputDir}/${name}.json`;

  let converted: Record<string, unknown>[] = xlsx.utils.sheet_to_json(sheet);
  console.log(converted[0]);
  if (include_row_fn) {
    converted = converted.filter((r) => include_row_fn(r));
  }

  const outputFile = Bun.file(outputPath);

  await outputFile.write(JSON.stringify(converted));
}
