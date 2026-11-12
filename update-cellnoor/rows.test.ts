import { describe, expect, test } from "bun:test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import type { Api } from "./api.ts";
import {
  buildEachWithMeasurements,
  emails,
  optionalNumber,
  startOfWorkday,
  withCanonicalColumns,
} from "./rows.ts";

describe("withCanonicalColumns", () => {
  test("maps single headers and lists of headers, keeping the raw row", () => {
    const row = {
      ID: "25SP1",
      "Preparer 1 Email": "a@jax.org",
      "Preparer 2": "",
      "Preparer 3": "b@jax.org",
    };

    expect(
      withCanonicalColumns(row, {
        readable_id: "ID",
        preparers: ["Preparer 1 Email", "Preparer 2", "Preparer 3"],
      }),
    ).toEqual({
      ...row,
      readable_id: "25SP1",
      preparers: ["a@jax.org", "b@jax.org"],
    });
  });
});

describe("cell schemas", () => {
  test("emails are lowercased and deduplicated", () => {
    expect(emails.parse(["A@jax.org", "a@jax.org", "b@jax.org"])).toEqual([
      "a@jax.org",
      "b@jax.org",
    ]);
  });

  test("dates become 9 AM Eastern Standard Time on the calendar day", () => {
    expect(startOfWorkday.parse(new Date(2025, 1, 6))).toBe(
      "2025-02-06T14:00:00.000Z",
    );
  });

  test("0 and empty cells are not measurements", () => {
    expect(optionalNumber.parse(0)).toBeNull();
    expect(optionalNumber.parse("")).toBeNull();
    expect(optionalNumber.parse(1.5)).toBe(1.5);
  });
});

describe("buildEachWithMeasurements", () => {
  test("a record whose measurements can't be built is marked, so its existing measurements aren't deleted", async () => {
    const api = {
      errorsDir: mkdtempSync(`${tmpdir()}/update-cellnoor-`),
      problems: [],
    } as unknown as Api;

    const built = await buildEachWithMeasurements(
      api,
      { record: "suspension", measurements: "suspension" },
      ["good", "bad"],
      (id) => id,
      (id) => ({ readable_id: id }),
      (id) => {
        if (id === "bad") {
          throw new Error("unknown counting method");
        }
        return [1];
      },
    );

    expect(built).toEqual([
      {
        body: { readable_id: "good", measurements: [1] },
        measurementsFailed: false,
      },
      {
        body: { readable_id: "bad", measurements: [] },
        measurementsFailed: true,
      },
    ]);
    expect(api.problems).toHaveLength(1);
  });
});
