import { send, type Api, type FetchResult } from "./api.ts";
import { diff, matches } from "./compare.ts";

export async function syncMeasurements<Measurement extends object>(
  api: Api,
  identifier: string,
  desired: Measurement[],
  actual: { id: string }[],
  requests: {
    add: (measurement: Measurement) => Promise<FetchResult<unknown>>;
    remove: (ids: string[]) => Promise<FetchResult<unknown>>;
  },
): Promise<void> {
  const { missing, extra } = diff(desired, actual, matches);

  for (const measurement of missing) {
    await send(api, identifier, measurement, requests.add(measurement));
  }

  if (extra.length > 0) {
    const ids = extra.map(({ id }) => id);
    await send(api, identifier, ids, requests.remove(ids));
  }
}

export async function syncIds(
  api: Api,
  identifier: string,
  desired: string[],
  actual: string[],
  requests: {
    add: (ids: string[]) => Promise<FetchResult<unknown>>;
    remove: (ids: string[]) => Promise<FetchResult<unknown>>;
  },
): Promise<void> {
  const { missing, extra } = diff(desired, actual, (d, a) => d === a);

  if (missing.length > 0) {
    await send(api, identifier, missing, requests.add(missing));
  }

  if (extra.length > 0) {
    await send(api, identifier, extra, requests.remove(extra));
  }
}
