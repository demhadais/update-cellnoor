import { send, type Api, type FetchResult } from "./api.ts";
import { diff, matches } from "./compare.ts";
import type { Subject } from "./report.ts";

export async function syncMeasurements<Measurement extends object>(
  api: Api,
  subject: Subject,
  desired: Measurement[],
  actual: { id: string }[],
  requests: {
    add: (measurement: Measurement) => Promise<FetchResult<unknown>>;
    remove: (ids: string[]) => Promise<FetchResult<unknown>>;
  },
): Promise<void> {
  const { missing, extra } = diff(desired, actual, matches);

  for (const measurement of missing) {
    await send(api, subject, measurement, requests.add(measurement));
  }

  if (extra.length > 0) {
    const ids = extra.map(({ id }) => id);
    await send(api, subject, ids, requests.remove(ids));
  }
}

export async function syncIds(
  api: Api,
  subject: Subject,
  desired: string[],
  actual: string[],
  requests: {
    add: (ids: string[]) => Promise<FetchResult<unknown>>;
    remove: (ids: string[]) => Promise<FetchResult<unknown>>;
  },
): Promise<void> {
  const { missing, extra } = diff(desired, actual, (d, a) => d === a);

  if (missing.length > 0) {
    await send(api, subject, missing, requests.add(missing));
  }

  if (extra.length > 0) {
    await send(api, subject, extra, requests.remove(extra));
  }
}
