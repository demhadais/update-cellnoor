import { writeError, type Api, type FetchResult, type Schemas } from "./api.ts";

export type SeedData = {
  single_index_set_urls: string[];
  dual_index_set_urls: string[];
  tenx_assays: Schemas["NewTenxAssay"][];
  multiplexing_tags: Schemas["MultiplexingTag"][];
};

type TenxDualIndexSet = {
  "index(i7)": string;
  "index2_workflow_a(i5)": string;
  "index2_workflow_b(i5)": string;
};

export async function uploadSeedData(
  api: Api,
  seedData: SeedData,
): Promise<Set<string>> {
  const singleIndexSetNames = new Set<string>();

  for (const url of seedData.single_index_set_urls) {
    const response = await fetch(url);
    const indexSets = (await response.json()) as Schemas["NewSingleIndexSet"][];
    indexSets.forEach(([name]) => singleIndexSetNames.add(name));

    await sendIgnoringConflict(
      api,
      url,
      indexSets,
      api.client.POST("/index-sets/single", { body: indexSets }),
    );
  }

  for (const url of seedData.dual_index_set_urls) {
    const response = await fetch(url);
    const indexSets = toDualIndexSets(
      (await response.json()) as Record<string, TenxDualIndexSet>,
    );

    await sendIgnoringConflict(
      api,
      url,
      indexSets,
      api.client.POST("/index-sets/dual", { body: indexSets }),
    );
  }

  for (const assay of seedData.tenx_assays) {
    await sendIgnoringConflict(
      api,
      `${assay.name} ${assay.chemistry_version} ${assay.sample_multiplexing} ${assay.chromium_chip}`,
      assay,
      api.client.POST("/10x-assays", { body: assay }),
    );
  }

  for (const tag of seedData.multiplexing_tags) {
    await sendIgnoringConflict(
      api,
      `${tag.type} ${tag.tag_id}`,
      tag,
      api.client.POST("/multiplexing-tags", { body: tag }),
    );
  }

  return singleIndexSetNames;
}

export function toDualIndexSets(
  tenxIndexSets: Record<string, TenxDualIndexSet>,
): Record<string, Schemas["NewDualIndexSet"]> {
  return Object.fromEntries(
    Object.entries(tenxIndexSets).map(([name, indexSet]) => [
      name,
      {
        index_i7: indexSet["index(i7)"],
        index2_workflow_a_i5: indexSet["index2_workflow_a(i5)"],
        index2_workflow_b_i5: indexSet["index2_workflow_b(i5)"],
      },
    ]),
  );
}

async function sendIgnoringConflict(
  api: Api,
  identifier: string,
  request: unknown,
  call: Promise<FetchResult<unknown>>,
): Promise<void> {
  const { error, response } = await call;

  if (response.ok || response.status === 409) {
    return;
  }

  await writeError(api, identifier, {
    request,
    response: { status: response.status, body: error },
  });
}
