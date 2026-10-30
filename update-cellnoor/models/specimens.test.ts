import { describe, expect, test } from "bun:test";
import { speciesFields, typeFields } from "./specimens.ts";

describe("speciesFields", () => {
  test("a PDX specimen has a host species", () => {
    expect(speciesFields("Homo sapiens + Mus musculus (PDX)")).toEqual({
      species: "homo_sapiens",
      host_species: "mus_musculus",
    });
  });

  test("capitalization doesn't matter", () => {
    expect(speciesFields("Mus Musculus")).toEqual({
      species: "mus_musculus",
      host_species: null,
    });
  });
});

describe("typeFields", () => {
  test("a curl is a block", () => {
    expect(
      typeFields({
        type: "Curl",
        preservation_method: "Flash-frozen",
        embedded_in: "OCT",
      }),
    ).toEqual({
      type: "block",
      embedded_in: "optimal_cutting_temperature_compound",
      fixative: undefined,
      thermal_preservation_method: "flash_freezing",
    });
  });

  test("a fixed suspension carries its fixative", () => {
    expect(
      typeFields({
        type: "Nucleus Suspension",
        preservation_method: "DSP-fixed",
        embedded_in: null,
      }),
    ).toEqual({
      type: "suspension",
      preservation_state: "fixed",
      fixative: "dithiobis_succinimidylpropionate",
    });
  });

  test("an unknown type is rejected", () => {
    expect(() =>
      typeFields({
        type: "Slide",
        preservation_method: null,
        embedded_in: null,
      }),
    ).toThrow("unknown specimen type 'Slide'");
  });
});
