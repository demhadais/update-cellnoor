import { test, expect } from "bun:test";
import data from "./config.sample.toml";
import { ConfigValidator, readConfig } from "./config";

const sampleConfig = {
  microsoft_tenant_id: "tenant",
  microsoft_client_id: "client",
  microsoft_client_secret: "secret",
  microsoft_sharepoint_site_id: "site-id",
  workbooks: [
    {
      file_path:
        "LIMS and Tracking/Tracking Sheets - In Use/People and Institutions.xlsx",
      sheets: [
        { name: "Institutions" },
        {
          name: "People",
          include_row_fn:
            "(row) => row['Full Name'] !== ' ' && row['microsoft_entra_oid'] !== 0",
        },
      ],
    },
  ],
};

test("config parsing", () => {
  expect(data).toStrictEqual(sampleConfig);
});

test("config validation", () => {
  const validatedConfig = ConfigValidator.parse(sampleConfig);
  const includeRow = validatedConfig.workbooks[0]!.sheets[1]!.include_row_fn!;

  expect(includeRow({ "Full Name": " " })).toBeFalse();
});
