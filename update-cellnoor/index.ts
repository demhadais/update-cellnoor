import {
  createMicrosoftGraphClient,
  downloadWorkbook,
} from "./microsoft-graph-client.ts";
import { readConfig, type Config, type WorkbookKey } from "./config.ts";
import { mkdirSync } from "fs";
import { exportWorkbook } from "./export-workbook.ts";
import { createApi } from "./api.ts";
import { uploadSeedData, type SeedData } from "./seed.ts";
import seedData from "../seed-data.json";
import { syncInstitutions } from "./models/institutions.ts";
import { syncPeople } from "./models/people.ts";
import { syncProjects } from "./models/projects.ts";
import { syncSpecimens } from "./models/specimens.ts";
import { syncSuspensions } from "./models/suspensions.ts";
import { syncSuspensionPools } from "./models/suspension-pools.ts";
import { syncChromiumRuns } from "./models/chromium-runs.ts";
import { syncCdna } from "./models/cdna.ts";
import { syncLibraries } from "./models/libraries.ts";
import { syncChromiumDatasets } from "./models/chromium-datasets.ts";

const WORKBOOK_KEYS: WorkbookKey[] = [
  "people_and_institutions",
  "sample_inventory",
  "chromium",
];

async function main() {
  const configPath =
    Bun.env.UPDATE_CELLNOOR_CONFIG_PATH ??
    "/sc/service/.config/update-cellnoor/config.toml";
  const outputDir = Bun.env.UPDATE_CELLNOOR_OUTPUT_DIR ?? "update-cellnoor-out";
  const datasetDirs = Bun.argv.slice(2);

  const config = await readConfig(configPath);
  const { workbooks } = config;

  // Do this cheap step before the expensive step of downloading the files
  mkdirSync(outputDir, { recursive: true });

  const rawFiles = await readWorkbooks(config);

  const [peopleAndInstitutions, sampleInventory, chromium] = await Promise.all([
    rawFiles.people_and_institutions &&
      exportWorkbook(
        rawFiles.people_and_institutions,
        workbooks.people_and_institutions.sheets,
        outputDir,
      ),
    rawFiles.sample_inventory &&
      exportWorkbook(
        rawFiles.sample_inventory,
        workbooks.sample_inventory.sheets,
        outputDir,
      ),
    rawFiles.chromium &&
      exportWorkbook(rawFiles.chromium, workbooks.chromium.sheets, outputDir),
  ]);

  if (!peopleAndInstitutions || !sampleInventory || !chromium) {
    console.log(
      "Not every workbook was provided, so the cellnoor API will not be updated",
    );
    return;
  }

  const api = createApi({
    baseUrl: config.api_base_url,
    filesBaseUrl: config.files_base_url,
    apiKey: config.api_key,
    outputDir,
  });

  const singleIndexSetNames = await uploadSeedData(api, seedData as SeedData);

  const institutionIds = await syncInstitutions(
    api,
    peopleAndInstitutions.institutions,
  );
  const personIds = await syncPeople(
    api,
    peopleAndInstitutions.people,
    institutionIds,
  );
  const projectIds = await syncProjects(
    api,
    peopleAndInstitutions.labs,
    personIds,
  );

  const specimens = await syncSpecimens(
    api,
    {
      specimens: sampleInventory.specimens,
      qcMeasurements: sampleInventory.qc_measurements,
    },
    { people: personIds, projects: projectIds },
  );

  const suspensionIds = await syncSuspensions(api, chromium.suspensions, {
    people: personIds,
    specimens,
  });
  const poolIds = await syncSuspensionPools(
    api,
    {
      pools: chromium.multiplexed_suspensions,
      suspensions: chromium.suspensions,
    },
    { people: personIds, suspensions: suspensionIds },
  );
  const gemWellIds = await syncChromiumRuns(
    api,
    { gems: chromium.gems, gemsSuspensions: chromium.gems_suspensions },
    {
      people: personIds,
      suspensions: suspensionIds,
      pools: poolIds,
      assayMap: config.assay_map,
    },
  );
  const cdnaIds = await syncCdna(api, chromium.cdna, {
    people: personIds,
    gemWells: gemWellIds,
  });
  const libraryIds = await syncLibraries(api, chromium.libraries, {
    people: personIds,
    cdna: cdnaIds,
    singleIndexSetNames,
  });

  await syncChromiumDatasets(api, datasetDirs, libraryIds);
}

async function readWorkbooks(
  config: Config,
): Promise<Partial<Record<WorkbookKey, Uint8Array>>> {
  const localPaths = WORKBOOK_KEYS.map(
    (key) => [key, Bun.env[`UPDATE_CELLNOOR_${key.toUpperCase()}`]] as const,
  );

  if (localPaths.some(([_, path]) => path)) {
    const reads = localPaths.flatMap(([key, path]) =>
      path ? [Bun.file(path).bytes().then((bytes) => [key, bytes] as const)] : [],
    );
    return Object.fromEntries(await Promise.all(reads));
  }

  const client = createMicrosoftGraphClient({
    tenantId: config.microsoft_tenant_id,
    clientId: config.microsoft_client_id,
    clientSecret: config.microsoft_client_secret,
  });

  const downloads = WORKBOOK_KEYS.map(async (key) => {
    const rawFile = await downloadWorkbook(client, {
      siteId: config.microsoft_sharepoint_site_id,
      filePath: config.workbooks[key].file_path,
    });
    return [key, await rawFile.bytes()] as const;
  });

  return Object.fromEntries(await Promise.all(downloads));
}

await main();
