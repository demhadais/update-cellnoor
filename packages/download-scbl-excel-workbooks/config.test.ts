import { expect, test } from "bun:test";
import { ConfigValidator } from "./config.ts";
import sampleConfigToml from "./config.sample.toml";

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
          exclude_row_fn:
            "(row) => row['Full Name'] === ' ' || row['microsoft_entra_oid'] === '0'",
        },
      ],
    },
  ],
};

test("config parsing", () => {
  expect(sampleConfigToml).toStrictEqual(sampleConfigObj);
});

test("config validation", () => {
  const validatedConfig = ConfigValidator.parse(sampleConfigObj);
  const excludeRow = validatedConfig.workbooks[0]!.sheets[1]!
    .exclude_row_fn!;

  expect(excludeRow({ "Full Name": " " })).toBeTrue();
});
