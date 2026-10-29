import {
  createCellnoorClient,
  type CellnoorClient,
} from "cellnoor/packages/cellnoor-client/index.ts";
import type { components } from "cellnoor/packages/cellnoor-client/cellnoor-types.ts";
import { mkdirSync, rmSync } from "fs";

export type Schemas = components["schemas"];

export type Api = {
  client: CellnoorClient;
  baseUrl: string;
  filesBaseUrl: string;
  headers: Record<string, string>;
  errorsDir: string;
};

export function createApi({
  baseUrl,
  filesBaseUrl,
  apiKey,
  outputDir,
}: {
  baseUrl: string;
  filesBaseUrl: string;
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
    filesBaseUrl,
    headers,
    errorsDir,
  };
}

export type FetchResult<T> = {
  data?: T;
  error?: unknown;
  response: Response;
};

export async function send<T>(
  api: Api,
  identifier: string,
  request: unknown,
  call: Promise<FetchResult<T>>,
): Promise<T | undefined> {
  const { data, error, response } = await call;

  if (response.ok) {
    return data;
  }

  await writeError(api, identifier, {
    request,
    response: { status: response.status, body: error },
  });
}

export async function sendSucceeded(
  api: Api,
  identifier: string,
  request: unknown,
  call: Promise<FetchResult<unknown>>,
): Promise<boolean> {
  const { error, response } = await call;

  if (!response.ok) {
    await writeError(api, identifier, {
      request,
      response: { status: response.status, body: error },
    });
  }

  return response.ok;
}

export async function reportLocalError(
  api: Api,
  identifier: string,
  request: unknown,
  error: unknown,
): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await writeError(api, identifier, { request, response: { error: message } });
}

export async function writeError(
  api: Api,
  identifier: string,
  content: { request: unknown; response: unknown },
): Promise<void> {
  const base = `${api.errorsDir}/${identifier.replaceAll("/", "_")}`;

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
