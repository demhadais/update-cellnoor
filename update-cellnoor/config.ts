import * as z from "zod";

const SheetSpecification = z.object({
  name: z.string(),
  header: z.number().default(0),
  exclude_row_fn: z
    .preprocess(
      eval,
      z.function({
        input: [z.record(z.string(), z.unknown())],
        output: z.boolean(),
      }),
    )
    .optional(),
  date_columns: z.array(z.string()).optional(),
  columns: z
    .record(z.string(), z.union([z.string(), z.array(z.string())]))
    .default({}),
});

const PeopleAndInstitutionsWorkbook = z.object({
  file_path: z.string(),
  sheets: z.object({
    institutions: SheetSpecification,
    people: SheetSpecification,
    labs: SheetSpecification,
  }),
});

const SampleInventoryWorkbook = z.object({
  file_path: z.string(),
  sheets: z.object({
    specimens: SheetSpecification,
    qc_measurements: SheetSpecification,
  }),
});

const ChromiumWorkbook = z.object({
  file_path: z.string(),
  sheets: z.object({
    suspensions: SheetSpecification,
    multiplexed_suspensions: SheetSpecification,
    gems: SheetSpecification,
    gems_suspensions: SheetSpecification,
    cdna: SheetSpecification,
    libraries: SheetSpecification,
  }),
});

const AssaySpecification = z.object({
  name: z.string(),
  library_types: z.array(z.string()),
  sample_multiplexing: z.string(),
  chemistry_version: z.string(),
  chromium_chip: z.string(),
});

export const ConfigValidator = z.object({
  microsoft_tenant_id: z.string(),
  microsoft_client_id: z.string(),
  microsoft_client_secret: z.string(),
  microsoft_sharepoint_site_id: z.string(),
  api_base_url: z.string(),
  files_base_url: z.string(),
  api_key: z.string(),
  workbooks: z.object({
    people_and_institutions: PeopleAndInstitutionsWorkbook,
    sample_inventory: SampleInventoryWorkbook,
    chromium: ChromiumWorkbook,
  }),
  assay_map: z.record(z.string(), AssaySpecification),
});

export async function readConfig(path: string): Promise<Config> {
  return ConfigValidator.parse(await import(path));
}

export type SheetSpecification = z.infer<typeof SheetSpecification>;

export type AssaySpecification = z.infer<typeof AssaySpecification>;

export type Config = z.infer<typeof ConfigValidator>;

export type WorkbookKey = keyof Config["workbooks"];

export type WorkbookSpecification = Config["workbooks"][WorkbookKey];
