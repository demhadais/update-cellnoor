import * as z from "zod";

const SheetSpecification = z.object({
  name: z.string(),
  include_row_fn: z
    .preprocess(
      eval,
      z.function({
        input: [z.record(z.string(), z.unknown())],
        output: z.boolean(),
      }),
    )
    .optional(),
});

const WorkbookSpecification = z.object({
  file_path: z.string(),
  sheets: z.array(SheetSpecification),
});

export const ConfigValidator = z.object({
  microsoft_tenant_id: z.string(),
  microsoft_client_id: z.string(),
  microsoft_client_secret: z.string(),
  microsoft_sharepoint_site_id: z.string(),
  workbooks: z.array(WorkbookSpecification),
});

export async function readConfig(path: string) {
  return ConfigValidator.parse(await import(path));
}

export type SheetSpecification = z.infer<typeof SheetSpecification>;

export type WorkbookSpecification = z.infer<typeof WorkbookSpecification>;

export type Config = z.infer<typeof ConfigValidator>;
