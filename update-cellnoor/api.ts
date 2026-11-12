import {
  createCellnoorClient,
  type CellnoorClient,
} from "cellnoor/packages/cellnoor-client/index.ts";
import type { components } from "cellnoor/packages/cellnoor-client/cellnoor-types.ts";
import { mkdirSync, rmSync } from "fs";
import { problemFromError, type Problem, type Subject } from "./report.ts";

export type Schemas = components["schemas"];

export type Api = {
  client: CellnoorClient;
  baseUrl: string;
  headers: Record<string, string>;
  errorsDir: string;
  problems: Problem[];
};

export function createApi({
  baseUrl,
  apiKey,
  outputDir,
}: {
  baseUrl: string;
  apiKey: string;
  outputDir: string;
}): Api {
  const headers: Record<string, string> = apiKey ? { "x-api-key": apiKey } : {};

  const errorsDir = `${outputDir}/errors`;
  rmSync(errorsDir, { recursive: true, force: true });
  mkdirSync(errorsDir, { recursive: true });

  return {
    client: createCellnoorClient({ baseUrl, headers }),
    baseUrl,
    headers,
    errorsDir,
    problems: [],
  };
}

export type ApiError =
  | Schemas["JsonRejection"]
  | Schemas["AuthError"]
  | Schemas["DbError"]
  | Schemas["RemovePreparersError"]
  | Schemas["IndexSetError"]
  | Schemas["PersonError"]
  | Schemas["CreateChromiumDatasetError"]
  | Schemas["DeleteChromiumDatasetError"];

export type FetchResult<T> = {
  data?: T;
  error?: ApiError;
  response: Response;
};

export async function send<T>(
  api: Api,
  subject: Subject,
  request: unknown,
  call: Promise<FetchResult<T>>,
): Promise<T | undefined> {
  let result: FetchResult<T>;
  try {
    result = await call;
  } catch (thrown) {
    await reportLocalError(api, subject, request, thrown);
    return undefined;
  }
  const { data, error, response } = result;

  if (response.ok) {
    return data;
  }

  await reportApiError(api, subject, request, response.status, error);
}

export async function reportApiError(
  api: Api,
  subject: Subject,
  request: unknown,
  status: number,
  body: ApiError | string | undefined,
): Promise<void> {
  api.problems.push({ type: "rejected", subject, body });
  await writeError(api, subject, { request, response: { status, body } });
}

export async function reportLocalError(
  api: Api,
  subject: Subject,
  request: unknown,
  error: unknown,
): Promise<void> {
  api.problems.push(problemFromError(subject, error));
  const message = error instanceof Error ? error.message : String(error);
  await writeError(api, subject, { request, response: { error: message } });
}

export function reportParentFailed(
  api: Api,
  subject: Subject,
  parent: Subject,
): void {
  api.problems.push({ type: "parent_failed", subject, parent });
}

export async function writeError(
  api: Api,
  { entity, id }: Subject,
  content: { request: unknown; response: unknown },
): Promise<void> {
  const base = `${api.errorsDir}/${entity}_${id.replaceAll(/[\s/]/g, "_")}`;

  let path = `${base}.json`;
  for (let n = 2; await Bun.file(path).exists(); n++) {
    path = `${base}-${n}.json`;
  }

  await Bun.write(path, JSON.stringify(content, null, 2));
}

export async function fetchOrThrow<T>(
  call: Promise<FetchResult<T>>,
): Promise<T> {
  const { data, error, response } = await call;

  if (!response.ok || data === undefined) {
    throw new Error(
      `${response.url} returned ${response.status}: ${JSON.stringify(error)}`,
    );
  }

  return data;
}
