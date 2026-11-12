import { expect, test } from "bun:test";
import { ConfigValidator } from "./config.ts";
import { renderReport, type Problem } from "./report.ts";

const { workbooks } = ConfigValidator.parse(
  Bun.TOML.parse(
    await Bun.file(`${import.meta.dir}/../config.sample.toml`).text(),
  ),
);

test("a reference to a record with its own problem isn't repeated", () => {
  const problems: Problem[] = [
    {
      type: "missing_value",
      subject: { entity: "specimen", id: "25SP1" },
      field: "tissue",
    },
    {
      type: "unknown_reference",
      subject: { entity: "suspension", id: "25E1-S1" },
      reference: { entity: "specimen", id: "25SP1" },
    },
    {
      type: "unknown_reference",
      subject: { entity: "suspension", id: "25E1-S2" },
      reference: { entity: "specimen", id: "25SP2" },
    },
    {
      type: "other",
      subject: { entity: "run", id: "stopped early" },
      message: "connection refused",
    },
  ];

  expect(renderReport(problems, workbooks, new Map())).toBe(
    [
      `In the "Specimens" sheet of the "Sample Inventory" workbook, the following specimens have nothing in the "Tissue" column:`,
      "- 25SP1",
      `Please fill in "Tissue" for these specimens.`,
      "",
      `In the "Suspensions" sheet of the "Chromium" workbook, the following suspensions refer to specimens that could not be found:`,
      "- 25E1-S2: 25SP2",
      `Please correct these, or add the missing specimens to the "Specimens" sheet of the "Sample Inventory" workbook.`,
      "",
      "1 other problem can't be fixed by editing the workbooks.",
      "",
    ].join("\n"),
  );
});
