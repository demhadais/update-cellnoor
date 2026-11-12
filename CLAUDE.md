# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Bun/TypeScript CLI that downloads the Single Cell Biology Laboratory's experiment-tracking Excel workbooks from SharePoint, parses them, and syncs the result (plus Cell Ranger pipeline outputs) into the cellnoor REST API. All code lives in `update-cellnoor/`; run every command from there.

## Commands

```sh
bun install
bun test                                   # all tests
bun test models/specimens.test.ts          # one file
bun test -t "a curl is a block"            # tests whose name matches a regex
bun run typecheck                          # = bun check
```

CI (`.github/workflows/typescript-ci.yml`) runs `bun fmt --check` and `bun test`.

Running the pipeline:

```sh
UPDATE_CELLNOOR_CONFIG_PATH=../config.toml \
UPDATE_CELLNOOR_OUTPUT_DIR=out \
bun index.ts [dataset-dir ...]
```

- Config defaults to `/sc/service/.config/update-cellnoor/config.toml`; `config.sample.toml` at the repo root is a complete example. Its `api_base_url` is the production API, and a full run issues PUTs and DELETEs.
- Setting any of `UPDATE_CELLNOOR_PEOPLE_AND_INSTITUTIONS`, `UPDATE_CELLNOOR_SAMPLE_INVENTORY`, `UPDATE_CELLNOOR_CHROMIUM` to a local `.xlsx` path skips the SharePoint download and reads only the workbooks given. If not all three are given, the run exports the sheets to JSON and exits without contacting the API. `examples/input-workbooks/` has sample workbooks.
- Positional args are Cell Ranger output directories to upload as chromium datasets.

Release: pushing a SemVer tag triggers cargo-dist (`dist-workspace.toml`, `.github/workflows/release.yml`), which runs `bun dist.ts` to `bun build --compile` a linux-x64 binary and publishes a shell installer plus an `update-cellnoor-update` updater. `scripts/prod/update-cellnoor.elion2.sh` is the production invocation.

## Architecture

`index.ts` is a linear pipeline:

1. **Config** (`config.ts`): the TOML file is loaded with Bun's `import()` and validated with zod. Each sheet spec has a `columns` map from canonical name to workbook header (or a list of headers, which become an array with blanks dropped), an optional `header` row offset, and an optional `exclude_row_fn` — a JS source string that is `eval`'d into a row filter. Workbook header renames are fixed in config, not code.
2. **Export** (`export-workbook.ts`): SheetJS parses each sheet, applies `exclude_row_fn`, writes the raw rows to `<outputDir>/<sheet name>.json`, then adds the canonical keys to each row (original keys are kept).
3. **Seed** (`seed.ts`): uploads `../seed-data.json` (index sets fetched from 10x URLs, 10x assays, multiplexing tags). 409s are ignored, so this is idempotent.
4. **Sync models** (`models/*.ts`) in dependency order: institutions → people → projects (the "Labs" sheet) → specimens → suspensions → delete specimens without suspensions → suspension pools → chromium runs (GEMs) → cDNA → libraries → chromium datasets. Each `syncX` returns a `Map` from the workbook's key (email, readable ID, email domain) to the API UUID, which downstream syncs use to resolve references.

### Model module pattern

Every `models/*.ts` follows the same shape:

- A zod `XRow` schema built from the cell preprocessors in `rows.ts` (`text`, `optionalText`, `email`, `date`, `optionalNumber`, `flag`, ...). Workbook formulas emit `0` or `""` for missing values, so `isBlank` treats `0` as blank: an optional number of `0` becomes `null`. Date cells are parsed with `startOfWorkday`/`endOfWorkday` (and their `optional*` variants), which send 09:00/16:00 of the cell's calendar day at `EASTERN_STANDARD_TIME` (a fixed `-05:00`, shared with chromium datasets' `delivered_at`). Every date field uses a start-of-workday schema except specimens' `returned_at`.
- `parseRows` → pure `buildX(row, ids)` returning an API body typed as `Schemas["NewX"]`. Workbook vocabulary is converted through `lookup` (ID maps, throws `UnknownReference`) and `translate` (string tables keyed by the canonical column name, throws `UnknownValue`). A build function that needs a cell the schema allows to be blank throws `MissingValue(field)`.
- `buildEach` / `buildEachWithMeasurements` catch per-item build errors. A record whose measurements fail to build is still synced, without measurements.
- Fetch existing records (usually `/x/detailed`), POST missing ones, PUT when `!matches(body, current)`, then reconcile child collections with `syncMeasurements` / `syncIds` (`sync.ts`), which POST missing and DELETE extra.

Tests (`*.test.ts` next to the module) cover the pure `build*`/translation functions; nothing tests against a live API.

### Comparison semantics (`compare.ts`)

`matches(desired, actual)` only checks keys present in `desired`. `equivalent` treats `null` and `undefined` as equal, compares ISO timestamps by instant, and compares numbers after `Math.fround` because the API stores 32-bit floats. When a `*Detailed` response names or shapes a field differently from the `New*` body (e.g. specimens return `type_`, omit `preservation_state` and `origin`), the update function builds an adjusted `comparable` object before calling `matches`.

### Error reporting (`api.ts`, `report.ts`)

Per-record failures never abort the run. Every error is reported against a `Subject` (`{ entity, id }`, where `id` is the key other records use to look this one up). Each is written as `{request, response}` JSON to `<outputDir>/errors/<entity>_<id>.json` (whitespace and `/` in the id become `_`; `-2`, `-3`, ... suffixes on collisions) and recorded as a typed `Problem` in `api.problems`. At the end of a run, `renderReport` groups the problems by type and entity into `<outputDir>/errors/report.txt`. The report's readers are non-technical workbook editors: `report.ts` maps each entity to its sheet and each field to its column header through the config, and only counts problems that can't be fixed in a workbook (entities without a sheet, and API rejections other than data constraints and duplicates). That directory is wiped at the start of each run. Use `send` for writes. Use `fetchOrThrow` only for the list GETs that every later step depends on; it aborts the run, and no report is written.

A failed `lookup` throws `UnknownReference`. The report omits it when the referenced subject has its own problem, so a failed parent isn't repeated for every descendant. When children look a parent up by a different key than the parent's subject (GEMs of a chromium run, email domains of an institution), the parent's sync calls `reportParentFailed` for those keys.

### cellnoor API dependency

`cellnoor` is a git dependency on the API's repository. Request/response types come from `cellnoor/packages/cellnoor-client/cellnoor-types.ts` (generated from the API's OpenAPI spec) and are exposed as `Schemas`. Comments in this repo cite cellnoor's DB migrations and Rust handlers under `node_modules/cellnoor/` for API behavior. The chromium dataset file-upload route is not in the OpenAPI spec, so `models/chromium-datasets.ts` calls it with raw `fetch`.

### Domain-specific behavior

- Institutions are assigned to people by email domain via the table in `models/institutions.ts`, not by a workbook column. JAX is created by an API migration with the nil UUID and is not synced.
- People's Microsoft Entra OIDs come from Microsoft Graph (`microsoft-graph-client.ts`), members only — guests are excluded because their OID differs from their home tenant's. If the lookup fails, the error is reported and people are synced without OIDs.
- Chromium datasets: library readable IDs are parsed from the first `_`-separated segment of the directory name; an existing dataset is matched by its set of library readable IDs. `delivered_at` comes from `pipeline-metadata.json` with a hardcoded `-05:00` offset. Only a whitelist of filenames is uploaded, and a file is re-uploaded only when its SHA-256 differs from the API's copy.
- `config.assay_map` maps the Chromium workbook's assay names to 10x assay fields for chromium runs.

## Data

`test-data/` is gitignored because it contains real production data. `*.xlsx` is gitignored except the three files in `examples/input-workbooks/`.
