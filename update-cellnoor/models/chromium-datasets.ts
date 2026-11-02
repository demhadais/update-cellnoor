import { basename } from "path";
import {
  fetchOrThrow,
  reportLocalError,
  send,
  sendSucceeded,
  writeError,
  type Api,
  type Schemas,
} from "../api.ts";
import { matches } from "../compare.ts";
import { lookup } from "../rows.ts";

const LIBRARY_READABLE_ID = /25E\d+-L\d+|26CH\d+-L\d+/g;

// The API rejects files with any other name (see cellnoor-api's
// handlers/chromium_datasets/upload_files/upload_path.rs)
const UPLOADABLE_FILENAMES = new Set([
  "metrics_summary.csv",
  "qc_library_metrics.csv",
  "qc_report.html",
  "qc_sample_metrics.csv",
  "summary.csv",
  "web_summary.html",
]);
const ATAC_SUMMARY = "cellranger-atac/summary.json";

type NewChromiumDataset = Schemas["NewChromiumDataset"];

// `name` is the path relative to the pipeline directory, which is how the API
// expects files to be named
export type DatasetFile = { path: string; name: string };

export function libraryReadableIds(datasetDir: string): string[] {
  const [libraries = ""] = basename(datasetDir).split("_");
  return [...libraries.matchAll(LIBRARY_READABLE_ID)].map(([id]) => id);
}

// `relativePaths` are relative to `datasetDir`, so they start with the
// pipeline directory
export function selectFiles(
  datasetDir: string,
  relativePaths: string[],
): DatasetFile[] {
  return relativePaths
    .filter(
      (p) =>
        p === ATAC_SUMMARY ||
        (/\.(csv|html)$/.test(p) && UPLOADABLE_FILENAMES.has(basename(p))),
    )
    .map((p) => ({
      path: `${datasetDir}/${p}`,
      name: p.slice(p.indexOf("/") + 1),
    }));
}

async function listFiles(datasetDir: string): Promise<DatasetFile[]> {
  const glob = new Bun.Glob("*/**/*.{csv,html,json}");
  const relativePaths = await Array.fromAsync(glob.scan({ cwd: datasetDir }));
  return selectFiles(datasetDir, relativePaths.toSorted());
}

async function getDatasetDeliveryTime(
  api: Api,
  datasetDir: string,
): Promise<Date> {
  // Just hardcode UTC-5 (Eastern Standard Time)
  const deliveredAt = await Bun.file(`${datasetDir}/pipeline-metadata.json`)
    .json()
    .then((md) => `${md.metadata_generated_date}-05:00`);

  return new Date(deliveredAt);
}

async function buildDataset(
  api: Api,
  datasetDir: string,
  libraryIds: Map<string, string>,
): Promise<NewChromiumDataset> {
  const delivered_at = await getDatasetDeliveryTime(api, datasetDir).then((d) =>
    d.toISOString(),
  );

  return {
    name: basename(datasetDir),
    delivered_at,
    library_ids: libraryReadableIds(datasetDir).map((id) =>
      lookup(libraryIds, id, "library"),
    ),
  };
}

function librariesKey(libraryReadableIds: string[]): string {
  return libraryReadableIds.toSorted().join(",");
}

export async function syncChromiumDatasets(
  api: Api,
  datasetDirs: string[],
  libraryIds: Map<string, string>,
): Promise<void> {
  const existing = await fetchOrThrow(
    api.client.GET("/chromium-datasets/detailed"),
  );
  const existingByLibraries = new Map(
    existing.map((d) => [
      librariesKey(d.libraries.map((l) => l.readable_id)),
      d,
    ]),
  );

  for (const datasetDir of datasetDirs) {
    try {
      const dataset = await buildDataset(api, datasetDir, libraryIds);
      const files = await listFiles(datasetDir);
      const current = existingByLibraries.get(
        librariesKey(libraryReadableIds(datasetDir)),
      );

      if (!current) {
        await createDataset(api, dataset, files);
      } else if (!(await filesMatch(api, current, files))) {
        await replaceFiles(api, current, dataset, files);
      } else {
        await updateDataset(api, dataset, current);
      }
    } catch (error) {
      await reportLocalError(api, basename(datasetDir), datasetDir, error);
    }
  }
}

async function createDataset(
  api: Api,
  dataset: NewChromiumDataset,
  files: DatasetFile[],
): Promise<void> {
  const created = await send(
    api,
    dataset.name,
    dataset,
    api.client.POST("/chromium-datasets", { body: dataset }),
  );

  if (created) {
    await uploadFiles(api, created.id, dataset.name, files);
  }
}

async function updateDataset(
  api: Api,
  dataset: NewChromiumDataset,
  current: Schemas["ChromiumDatasetDetailed"],
): Promise<void> {
  const { library_ids: _, ...record } = dataset;

  if (!matches(record, current)) {
    await send(
      api,
      dataset.name,
      record,
      api.client.PUT("/chromium-datasets/{id}", {
        params: { path: { id: current.id } },
        body: record,
      }),
    );
  }
}

async function filesMatch(
  api: Api,
  dataset: Schemas["ChromiumDatasetDetailed"],
  files: DatasetFile[],
): Promise<boolean> {
  const prefix = `${dataset.links.file_dir}/`;
  const links = new Map(
    dataset.links.raw_files.map((link) => [link.slice(prefix.length), link]),
  );

  if (links.size !== files.length) {
    return false;
  }

  for (const file of files) {
    const link = links.get(file.name);
    if (!link) {
      return false;
    }

    const response = await fetch(`${api.filesBaseUrl}${link}`, {
      headers: api.headers,
    });
    if (!response.ok) {
      throw new Error(`downloading ${link} returned ${response.status}`);
    }

    const remote = Buffer.from(await response.bytes());
    if (!remote.equals(await Bun.file(file.path).bytes())) {
      return false;
    }
  }

  return true;
}

// The API doesn't allow overwriting a dataset's files, so the dataset is
// deleted and recreated
async function replaceFiles(
  api: Api,
  current: Schemas["ChromiumDatasetDetailed"],
  dataset: NewChromiumDataset,
  files: DatasetFile[],
): Promise<void> {
  const deleted = await sendSucceeded(
    api,
    dataset.name,
    { delete: current.id },
    api.client.DELETE("/chromium-datasets/{id}", {
      params: { path: { id: current.id } },
    }),
  );

  if (deleted) {
    await createDataset(api, dataset, files);
  }
}

async function uploadFiles(
  api: Api,
  datasetId: string,
  identifier: string,
  files: DatasetFile[],
): Promise<void> {
  const body = new FormData();
  for (const file of files) {
    body.append("file", Bun.file(file.path), file.name);
  }

  const response = await fetch(
    `${api.baseUrl}/chromium-datasets/${datasetId}/files`,
    { method: "POST", headers: api.headers, body },
  );

  if (!response.ok) {
    await writeError(api, identifier, {
      request: files.map(({ name }) => name),
      response: { status: response.status, body: await response.text() },
    });
  }
}
