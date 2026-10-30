import { describe, expect, test } from "bun:test";
import { date, emails, optionalNumber, withCanonicalColumns } from "./rows.ts";

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

  test("dates become the calendar day at UTC midnight", () => {
    expect(date.parse(new Date(2025, 1, 6))).toBe("2025-02-06T00:00:00.000Z");
  });

  test("0 and empty cells are not measurements", () => {
    expect(optionalNumber.parse(0)).toBeNull();
    expect(optionalNumber.parse("")).toBeNull();
    expect(optionalNumber.parse(1.5)).toBe(1.5);
  });
});
