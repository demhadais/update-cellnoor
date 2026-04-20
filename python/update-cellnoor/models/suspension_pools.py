import asyncio
from collections.abc import Generator
from typing import Any

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    get_person_email_id_map,
)


def _parse_row(
    row: dict[str, Any],
    suspensions: dict[str, list[dict[str, Any]]],
    people: dict[str, str],
    multiplexing_tags: dict[str, str],
) -> dict[str, Any] | None:
    required_keys = {"readable_id", "name", "date_pooled"}

    data = {key: row[key] for key in required_keys if key in row}

    # Assign basic information
    if pooled_at := row.get("date_pooled"):
        data["pooled_at"] = pooled_at

    data["suspensions"] = []
    for susp in suspensions[data["readable_id"]]:
        if multiplexing_tag_id := susp.get("multiplexing_tag_id"):
            if "ob" in str(multiplexing_tag_id).lower():
                continue

            data["suspensions"].append(
                {
                    "suspension_id": susp["id"],
                    "tag_id": multiplexing_tags.get(multiplexing_tag_id),
                }
            )
        else:
            data["suspensions"].append(susp["id"])

    if not data["suspensions"]:
        return None

    if isinstance(data["suspensions"][0], dict):
        data["multiplexing_type"] = "exogenous_tag"
    else:
        data["multiplexing_type"] = "genetic"

    data["preparer_ids"] = [
        people[row[email_key]]
        for email_key in ["preparer_1_email", "preparer_2"]
        if row.get(email_key) is not None
    ]

    return data


async def csvs_to_new_suspension_pools(
    client: aiohttp.ClientSession,
    people_url: str,
    suspension_pool_url: str,
    suspensions_url: str,
    multiplexing_tags_url: str,
    suspension_pool_data: list[dict[str, Any]],
    suspension_csv_data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        tasks = (
            tg.create_task(get_person_email_id_map(client, people_url)),
            tg.create_task(client.get(suspension_pool_url, params=NO_LIMIT_QUERY)),
            tg.create_task(client.get(suspensions_url, params=NO_LIMIT_QUERY)),
            tg.create_task(client.get(multiplexing_tags_url)),
        )

    people, pre_existing_suspension_pools, suspensions_from_api, multiplexing_tags = (
        tasks[0].result(),
        tasks[1].result(),
        tasks[2].result(),
        tasks[3].result(),
    )

    pre_existing_suspension_pools = await pre_existing_suspension_pools.json()
    pre_existing_suspension_pools = {
        pool["readable_id"] for pool in pre_existing_suspension_pools
    }

    grouped_suspensions = {
        row["readable_id"]: []
        for row in suspension_pool_data
        if row["readable_id"] is not None
    }
    suspension_csv_data = {
        susp_row["readable_id"]: susp_row for susp_row in suspension_csv_data
    }  # pyright: ignore[reportAssignmentType]

    suspensions_from_api = await suspensions_from_api.json()
    for suspension in suspensions_from_api:
        readable_id = suspension["readable_id"]
        suspension_from_csv = suspension_csv_data[readable_id]
        if pooled_into := suspension_from_csv.get("pooled_into_id"):
            suspension["pooled_into_id"] = pooled_into

            if multiplexing_tag := suspension_from_csv.get("multiplexing_tag_id"):
                suspension["multiplexing_tag_id"] = multiplexing_tag

            pooled_suspension_list = grouped_suspensions[pooled_into]
            pooled_suspension_list.append(suspension)

    multiplexing_tags = await multiplexing_tags.json()
    multiplexing_tags = {tag["tag_id"]: tag["id"] for tag in multiplexing_tags}
    new_suspension_pools = (
        _parse_row(
            row,
            suspensions=grouped_suspensions,
            people=people,
            multiplexing_tags=multiplexing_tags,
        )
        for row in suspension_pool_data
    )
    new_suspension_pools = (
        pool
        for pool in new_suspension_pools
        if not (pool is None or pool["readable_id"] in pre_existing_suspension_pools)
    )
    return new_suspension_pools
