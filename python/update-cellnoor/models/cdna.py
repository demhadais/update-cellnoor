import asyncio
import uuid
from collections.abc import Generator
from typing import Any

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    get_person_email_id_map,
    str_to_bool,
    str_to_float,
    to_snake_case,
)


def _parse_row(
    row: dict[str, Any],
    gem_pools: dict[str, str],
    people: dict[str, str],
) -> dict[str, Any] | None:
    data = {"readable_id": row["readable_id"]}

    # TODO
    data["measurements"] = []

    library_type = to_snake_case(row.get("library_type", ""))
    library_type = {
        "gene_expression_flex": "gene_expression",
        "vdj-t": "vdj",
        "vdj-b": "vdj",
    }.get(library_type, library_type)
    data["library_type"] = library_type

    data["preparers"] = [
        people[row[key]]
        for key in ["preparer_email", "preparer_2"]
        if key in row and row[key] is not None
    ]

    data["gem_well_id"] = gem_pools.get(row["gems_readable_id"])

    data["n_amplification_cycles"] = row.get("n_amplification_cycles")

    if volume := row.get("volume_(µl)"):
        data["volume_µl"] = int(str_to_float(volume))

    if prepared_at := row.get("date_prepared"):
        data["prepared_at"] = prepared_at

    additional_data = {}
    for key in ["experiment_id", "failure_notes", "storage_location", "notes"]:
        if value := row.get(key):
            additional_data[key] = value

    for key in ["is_preamplification_product", "fails_quality_control"]:
        if key in row:
            additional_data[key] = row[key]

    data["additional_data"] = additional_data

    return data


async def csv_to_new_cdna(
    client: aiohttp.ClientSession,
    people_url: str,
    chromium_run_url: str,
    cdna_url: str,
    data: list[dict[str, Any]],
    id_key: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        chromium_runs = tg.create_task(
            client.post(chromium_run_url, params=NO_LIMIT_QUERY, json={})
        )
        pre_existing_cdna = tg.create_task(client.get(cdna_url, params=NO_LIMIT_QUERY))

    people, chromium_runs, pre_existing_cdna = (
        people.result(),
        await chromium_runs.result().json(),
        await pre_existing_cdna.result().json(),
    )

    pre_existing_cdna = {c["readable_id"]: c for c in pre_existing_cdna}

    gem_wells = {}
    for run in chromium_runs:
        for gem_well in run["gem_wells"]:
            gem_wells[gem_well["readable_id"]] = gem_well["id"]

    cdna = (_parse_row(row, gem_wells, people) for row in data)
    cdna = (c for c in cdna if not (c is None or c["readable_id"] in pre_existing_cdna))

    return cdna
