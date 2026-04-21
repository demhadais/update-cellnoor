import asyncio
import uuid
from collections.abc import Generator
from typing import Any
from uuid import UUID

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    get_person_email_id_map,
    str_to_bool,
    str_to_float,
    str_to_int,
    to_snake_case,
)


def _map_bad_specimens(row: dict[str, Any]) -> dict[str, Any]:
    map = {
        "25SP1819": "25SP1794",
        "25SP1820": "25SP1795",
        "25SP1821": "25SP1796",
        "25SP1822": "25SP1797",
        "25SP1823": "25SP1798",
        "25SP1824": "25SP1799",
        "25SP1825": "25SP1800",
        "25SP1826": "25SP1801",
    }

    if mapped_parent_specimen_id := map.get(row["parent_specimen_readable_id"]):
        row["parent_specimen_readable_id"] = mapped_parent_specimen_id

    return row


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, dict[str, Any]],
    people: dict[str, UUID],
) -> dict[str, Any] | None:
    row = _map_bad_specimens(row)

    data = {key: row[key] for key in ["readable_id"]}

    parent_specimen = specimens.get(
        row["parent_specimen_readable_id"], {"id": uuid.uuid7()}
    )
    data["parent_specimen_id"] = parent_specimen["id"]

    if date_created := row.get("date_created"):
        data["created_at"] = date_created

    try:
        data["content"] = to_snake_case(row["biological_material"])
    except AttributeError:
        pass

    data["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_1_email", "preparer_2"]
        if row.get(key) is not None
    ]

    if target_cell_recovery := row.get("target_cell_recovery"):
        data["target_cell_recovery"] = str_to_int(target_cell_recovery)

    if lysis_duration := row.get("lysis_duration_minutes"):
        data["lysis_duration_minutes"] = str_to_float(lysis_duration)

    data["additional_data"] = {
        key: row[key] for key in ["experiment_id", "notes"] if key in row
    }
    for key in ["fails_quality_control", "filtered_more_than_once"]:
        data["additional_data"][key] = str_to_bool(row[key])

    return data


async def csv_to_new_suspensions(
    client: aiohttp.ClientSession,
    people_url: str,
    specimens_url: str,
    suspensions_url: str,
    multiplexing_tags_url: str,
    data: list[dict[str, Any]],
    id_key: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people_task = tg.create_task(get_person_email_id_map(client, people_url))
        specimens_task = tg.create_task(
            client.get(specimens_url, params=NO_LIMIT_QUERY)
        )
        pre_existing_suspensions_task = tg.create_task(
            client.get(suspensions_url, params=NO_LIMIT_QUERY)
        )
        multiplexing_tags_task = tg.create_task(
            client.get(multiplexing_tags_url, params=NO_LIMIT_QUERY)
        )

    people = people_task.result()
    specimens = await specimens_task.result().json()
    pre_existing_suspensions = await pre_existing_suspensions_task.result().json()
    multiplexing_tags = await multiplexing_tags_task.result().json()
    specimens = {s["readable_id"]: s for s in specimens}
    pre_existing_suspensions = {s["readable_id"] for s in pre_existing_suspensions}
    multiplexing_tags = {tag["tag_id"]: tag["id"] for tag in multiplexing_tags}

    new_suspensions = (
        _parse_suspension_row(
            row,
            specimens=specimens,  # pyright: ignore[reportArgumentType]
            people=people,  # pyright: ignore[reportArgumentType]
        )
        for row in data
    )

    new_suspensions = (
        susp
        for susp in new_suspensions
        if not (susp is None or susp["readable_id"] in pre_existing_suspensions)
    )

    return new_suspensions
