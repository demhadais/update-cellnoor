// A comment to trigger CI
import os from "os";
import {
  createMicrosoftGraphClient,
  downloadWorkbook,
} from "./microsoft-graph-client";
import { parseArgs } from "util";
import { readConfig } from "./config";
import { mkdirSync } from "fs";
import { exportWorkbook } from "./write-workbook";

async function main() {
  const {
    values: { config_path, output_dir },
  } = parseCommandline();

  const config = await readConfig(config_path);

  const {
    microsoft_tenant_id,
    microsoft_client_id,
    microsoft_client_secret,
    microsoft_sharepoint_site_id,
    workbooks,
  } = config;

  // Do this cheap step before the expensive step of downloading the files
  mkdirIfNotExists(output_dir);

  const client = createMicrosoftGraphClient({
    tenantId: microsoft_tenant_id,
    clientId: microsoft_client_id,
    clientSecret: microsoft_client_secret,
  });

  const downloads = workbooks.map(({ file_path }) =>
    downloadWorkbook(client, {
      siteId: microsoft_sharepoint_site_id,
      filePath: file_path,
    }),
  );

  // Waiting for `Iterator.zip` to drop!
  const rawFiles = await Promise.all(downloads);
  const exports = rawFiles.map((rf, i) =>
    exportWorkbook(rf, workbooks[i]!.sheets, output_dir),
  );

  await Promise.all(exports);
}

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

function mkdirIfNotExists(outputDir: string) {
  try {
    mkdirSync(outputDir);
  } catch (error) {
    if ((error as { code: string }).code !== "EEXIST") {
      throw error;
    }
  }
}

await main();
