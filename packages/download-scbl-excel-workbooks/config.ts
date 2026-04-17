import * as z from "zod";

const DataSources = z.record(z.string(), z.array(z.string()));

const Config = z.object({
  cellnoor_api_base_url: z.string(),
  cellnoor_api_key: z.string(),
  microsoft_tenant_id: z.string(),
  microsoft_client_id: z.string(),
  microsoft_client_secret: z.string(),
  microsoft_sharepoint_site_id: z.string(),
  data_sources: DataSources,
});

export async function readConfig(path: string) {
  return Config.parseAsync(await import(path));
}

export type DataSources = z.infer<typeof DataSources>;
export type Config = z.infer<typeof Config>;
