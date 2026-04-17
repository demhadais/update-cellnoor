import os from "os";
import {
  createMicrosoftGraphClient,
  downloadFiles,
} from "./microsoft-graph-client";
import { parseArgs } from "util";
import { readConfig } from "./config";
import * as xlsx from "xlsx";
import { mkdirSync, readdirSync } from "fs";

async function main() {
  const {
    values: { config_path, output_dir },
  } = parseCommandline();

  const config = await readConfig(config_path);

  const { microsoft_tenant_id, microsoft_client_id, microsoft_client_secret } =
    config;

  const client = createMicrosoftGraphClient({
    tenantId: microsoft_tenant_id,
    clientId: microsoft_client_id,
    clientSecret: microsoft_client_secret,
  });

  const rawFiles = await downloadFiles(client, config);
  const exports = rawFiles.map((rf) => exportWorkbook(rf, output_dir));
  mkdirSync(output_dir);

  await Promise.all(exports);
}

await main();

function parseCommandline() {
  const options = parseArgs({
    args: Bun.argv,
    allowPositionals: true,
    options: {
      config_path: {
        type: "string",
        short: "c",
        default:
          Bun.env.DOWNLOAD_SCBL_WORKBOOKS_CONFIG_PATH ??
          `${os.homedir()}/.config/update-cellnoor/download-scbl-workbooks.settings.toml`,
      },
      output_dir: {
        type: "string",
        short: "o",
        default:
          Bun.env.DOWNLOAD_SCBL_WORKBOOKS_OUTPUT_PATH ?? `scbl-workbooks`,
      },
    },
  });

  return options;
}

async function exportWorkbook(
  { rawFile, sheetNames }: { rawFile: ReadableStream; sheetNames: string[] },
  outputDir: string,
) {
  const workbook = xlsx.read(await rawFile.bytes());

  for (const sheetName of sheetNames) {
    const sheet = workbook.Sheets[sheetName];

    if (!sheet) {
      throw new Error(
        `sheet name ${sheetName} does not exist. Valid sheet names for this workbook: ${workbook.SheetNames}`,
      );
    }

    const outputPath = `${outputDir}/${sheetName}.json`;

    await exportToJson(sheet, outputPath);
  }
}

async function exportToJson(sheet: xlsx.WorkSheet, outputPath: string) {
  const converted = xlsx.utils.sheet_to_json(sheet);
  const outputFile = Bun.file(outputPath);

  await outputFile.write(JSON.stringify(converted));
}
