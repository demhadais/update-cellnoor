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

    library_type = to_snake_case(row["library_type"])
    library_type = {
        "gene_expression_flex": "gene_expression",
        "vdj-t": "vdj",
        "vdj-b": "vdj",
    }.get(library_type, library_type)
    data["library_type"] = library_type

    data["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_email", "preparer_2"]
        if key in row and row[key] is not None
    ]

    data["gem_pool_id"] = gem_pools.get(row["gems_readable_id"])

    if n_amplification_cycles := row.get("n_amplification_cycles"):
        data["n_amplification_cycles"] = int(str_to_float(n_amplification_cycles))
    else:
        data["n_amplification_cycles"] = 0

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
    gem_pool_url: str,
    cdna_url: str,
    data: list[dict[str, Any]],
    id_key: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        gem_pools = tg.create_task(client.get(gem_pool_url, params=NO_LIMIT_QUERY))
        pre_existing_cdna = tg.create_task(client.get(cdna_url, params=NO_LIMIT_QUERY))

    people, gem_pools, pre_existing_cdna = (
        people.result(),
        await gem_pools.result().json(),
        await pre_existing_cdna.result().json(),
    )

    pre_existing_cdna = {c["readable_id"]: c for c in pre_existing_cdna}

    gem_pools = {pool["readable_id"]: pool["id"] for pool in gem_pools}

    cdna = (_parse_row(row, gem_pools, people) for row in data)
    cdna = (c for c in cdna if not (c is None or c["readable_id"] in pre_existing_cdna))

    return cdna
