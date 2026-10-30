import { describe, expect, test } from "bun:test";
import { diff, equivalent, matches, unique } from "./compare.ts";

describe("equivalent", () => {
  test("null, undefined, and a missing key are the same", () => {
    expect(equivalent(null, undefined)).toBe(true);
    expect(equivalent({ a: 1, b: null }, { a: 1 })).toBe(true);
    expect(equivalent({ a: 1 }, { a: 1, b: undefined })).toBe(true);
    expect(equivalent({ a: 1, b: null }, { a: 1, b: 2 })).toBe(false);
  });

  test("timestamps are compared as instants", () => {
    expect(equivalent("2025-02-06T00:00:00.000Z", "2025-02-06T00:00:00Z")).toBe(
      true,
    );
    expect(
      equivalent("2025-02-06T00:00:00.000Z", "2025-02-06T01:00:00+01:00"),
    ).toBe(true);
    expect(equivalent("2025-02-06T00:00:00.000Z", "2025-02-07T00:00:00Z")).toBe(
      false,
    );
  });

  test("numbers are compared at the API's 32-bit precision", () => {
    expect(equivalent(90.6 / 100, 0.906)).toBe(true);
    expect(equivalent(0.906, 0.907)).toBe(false);
  });

  test("nested objects and arrays are compared deeply", () => {
    expect(
      equivalent(
        { data: { range: [150, 1000] } },
        { data: { range: [150, 1000] } },
      ),
    ).toBe(true);
    expect(
      equivalent(
        { data: { range: [150, 1000] } },
        { data: { range: [1000, 150] } },
      ),
    ).toBe(false);
    expect(equivalent({ notes: "a" }, { notes: "a", other: 1 })).toBe(false);
  });
});

describe("matches", () => {
  test("ignores fields that are only present in the actual entity", () => {
    expect(
      matches({ name: "x" }, { id: "1", name: "x", links: { self: "/x" } }),
    ).toBe(true);
    expect(matches({ name: "x" }, { name: "y" })).toBe(false);
  });
});

describe("diff", () => {
  test("treats lists as multisets", () => {
    const { missing, extra } = diff(
      [{ v: 1 }, { v: 1 }, { v: 2 }],
      [
        { id: "a", v: 1 },
        { id: "b", v: 3 },
      ],
      matches,
    );
    expect(missing).toEqual([{ v: 1 }, { v: 2 }]);
    expect(extra).toEqual([{ id: "b", v: 3 }]);
  });
});

describe("unique", () => {
  test("removes structurally identical items", () => {
    expect(unique([{ a: 1 }, { a: 1 }, { a: 2 }])).toEqual([
      { a: 1 },
      { a: 2 },
    ]);
  });
});
