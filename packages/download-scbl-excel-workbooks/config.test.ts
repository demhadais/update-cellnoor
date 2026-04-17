import { test, expect } from "bun:test";
import sampleConfigFile from "./config.sample.toml";
import { ConfigValidator, readConfig } from "./config";

const sampleConfigObj = {
  microsoft_tenant_id: "tenant",
  microsoft_client_id: "client",
  microsoft_client_secret: "secret",
  microsoft_sharepoint_site_id: "site-id",
  workbooks: [
    {
      file_path:
        "LIMS and Tracking/Tracking Sheets - In Use/People and Institutions.xlsx",
      sheets: [
        { name: "Institutions", header: 0 },
        {
          name: "People",
          header: 0,
          include_row_fn:
            "(row) => row['Full Name'] !== ' ' && row['microsoft_entra_oid'] !== 0",
        },
      ],
    },
  ],
};

test("config parsing", () => {
  expect(sampleConfigObj).toStrictEqual(sampleConfigObj);
});

test("config validation", () => {
  const validatedConfig = ConfigValidator.parse(sampleConfigObj);
  const includeRow = validatedConfig.workbooks[0]!.sheets[1]!.include_row_fn!;

  expect(includeRow({ "Full Name": " " })).toBeFalse();
});
