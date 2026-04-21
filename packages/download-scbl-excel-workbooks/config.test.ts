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
        { name: "Institutions" },
        {
          name: "People",
          exclude_row_fn:
            "(row) => !row['Full Name'] || row['Full Name'] === ' '",
        },
        { name: "Labs" },
      ],
    },
    {
      file_path:
        "LIMS and Tracking/Tracking Sheets - In Use/Sample Inventory.xlsx",
      sheets: [
        {
          name: "Specimens",
          exclude_row_fn:
            "(row) => !row['ID'] || !row['Submitter Email'] || new Set(['25SP116', '25SP117', '25SP1663', '25SP1664', '25SP1665', '25SP1666', '25SP1667', '25SP1668', '25SP1669', '25SP1670', '25SP1671', '25SP1672', '25SP1673', '25SP1674', '25SP1675', '25SP1676', '25SP1677', '25SP1678', '25SP1679', '25SP1680', '25SP1681', '25SP1682', '25SP1683', '25SP1684', '25SP1782', '25SP1783', '25SP1784', '25SP1785', '25SP1786', '25SP1787', '25SP1788', '25SP1789', '25SP1790', '25SP1791', '25SP1792', '25SP1793', '26SP0095', '26SP0096', '26SP0097', '26SP0098', '26SP0099', '26SP0100', '26SP0101', '26SP0102', '26SP0103', '26SP0104', '26SP0105', '26SP0106', '26SP0107', '26SP0108', '26SP0109']).has(row['ID'])",
        },
        {
          name: "QC Measurements",
          exclude_row_fn: "(row) => !row['Specimen ID']",
        },
      ],
    },
    {
      file_path: "LIMS and Tracking/Tracking Sheets - In Use/Chromium.xlsx",
      sheets: [
        {
          name: "Suspensions",
          header: 1,
          exclude_row_fn:
            "(row) => !row['Suspension ID'] || !row['Specimen ID'] || row['ID'] === '0'",
        },
        {
          name: "Multiplexed Suspensions",
          header: 1,
          exclude_row_fn: "(row) => !row['Experiment ID']",
        },
        {
          name: "GEMs",
          exclude_row_fn:
            "(row) => !row['Chromium Run ID'] || new Set(['G0072', 'G0073']).has(row['GEMs ID'])",
        },
        {
          name: "GEMs-Suspensions",
          exclude_row_fn: "(row) => !row['GEMs ID']",
        },
        {
          name: "cDNA",
          exclude_row_fn:
            "(row) => !row['GEMs ID'] || new Set(['25E28-C1', '25E28-C2', '25E103-C3', '25E103-C4']).has(row['cDNA/Pre-amplification Product ID'])",
        },
        {
          name: "Libraries",
          exclude_row_fn:
            "(row) => !row['Pre-amplified/cDNA ID'] || new Set(['25E28-L1', '25E28-L2']).has(row['Library ID'])",
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
