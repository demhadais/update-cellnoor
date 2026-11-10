import { describe, expect, test } from "bun:test";
import {
  libraryReadableIds,
  changedFiles,
  selectFiles,
  type DatasetFile,
} from "./chromium-datasets.ts";

describe("libraryReadableIds", () => {
  test("extracts every library ID before the specimen names", () => {
    expect(
      libraryReadableIds(
        "/sc/service/delivery/lab/10x-genomics/25E5-L1-25E5-L12_25E5-L3-specimen",
      ),
    ).toEqual(["25E5-L1", "25E5-L12"]);
    expect(libraryReadableIds("/delivery/26CH12-L10_specimen")).toEqual([
      "26CH12-L10",
    ]);
  });
});

describe("selectFiles", () => {
  test("keeps uploadable CSVs and HTMLs, and the cellranger-atac summary", () => {
    expect(
      selectFiles("/dataset", [
        "cellranger-multi/per_sample_outs/A/metrics_summary.csv",
        "cellranger-multi/per_sample_outs/A/web_summary.html",
        "cellranger-multi/per_sample_outs/A/count/feature_reference.csv",
        "cellranger-multi/config.json",
        "cellranger-multi/summary.json",
        "cellranger-atac/summary.json",
      ]),
    ).toEqual([
      {
        path: "/dataset/cellranger-multi/per_sample_outs/A/metrics_summary.csv",
        name: "per_sample_outs/A/metrics_summary.csv",
      },
      {
        path: "/dataset/cellranger-multi/per_sample_outs/A/web_summary.html",
        name: "per_sample_outs/A/web_summary.html",
      },
      { path: "/dataset/cellranger-atac/summary.json", name: "summary.json" },
    ]);
  });
});

describe("changedFiles", () => {
  const summary: DatasetFile = {
    path: "/dataset/cellranger-multi/summary.csv",
    name: "summary.csv",
  };
  const webSummary: DatasetFile = {
    path: "/dataset/cellranger-multi/per_sample_outs/A/web_summary.html",
    name: "per_sample_outs/A/web_summary.html",
  };

  test("returns nothing when every hash matches", () => {
    expect(
      changedFiles(
        new Map([
          ["summary.csv", "aa"],
          ["per_sample_outs/A/web_summary.html", "bb"],
        ]),
        [
          { file: summary, hash: "aa" },
          { file: webSummary, hash: "bb" },
        ],
      ),
    ).toEqual([]);
  });

  test("returns files whose hash differs or that are missing remotely", () => {
    expect(
      changedFiles(new Map([["summary.csv", "aa"]]), [
        { file: summary, hash: "cc" },
        { file: webSummary, hash: "bb" },
      ]),
    ).toEqual([summary, webSummary]);

    expect(
      changedFiles(new Map([["summary.csv", "aa"]]), [
        { file: summary, hash: "aa" },
        { file: webSummary, hash: "bb" },
      ]),
    ).toEqual([webSummary]);
  });

  test("ignores remote files that no longer exist locally", () => {
    expect(
      changedFiles(
        new Map([
          ["summary.csv", "aa"],
          ["per_sample_outs/B/web_summary.html", "bb"],
        ]),
        [{ file: summary, hash: "aa" }],
      ),
    ).toEqual([]);
  });
});
