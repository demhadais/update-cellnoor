import asyncio
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import aiohttp

from utils import (
    get_project_name_id_map,
)


def _parse_row(row: dict[str, Any]):
    required_keys = {"name"}

    data = {key: row[key] for key in required_keys if key in row}
    data["started_at"] = datetime(year=2014, month=1, day=1, tzinfo=UTC).isoformat()
    data["ended_at"] = datetime(
        year=2026, month=12, day=31, hour=23, minute=59, second=59, tzinfo=UTC
    ).isoformat()

    return data


async def csv_to_new_projects(
    client: aiohttp.ClientSession,
    project_url: str,
    data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        pre_existing_projects = tg.create_task(
            get_project_name_id_map(client, project_url)
        )

    pre_existing_projects = pre_existing_projects.result()

    new_projects = (_parse_row(row) for row in data)

    new_projects = (
        proj
        for proj in new_projects
        if not (proj is None or proj["name"] in pre_existing_projects)
    )

    return new_projects
