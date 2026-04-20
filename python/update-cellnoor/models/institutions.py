from collections.abc import Generator
from typing import Any

import aiohttp


def _parse_row(row: dict[str, Any]) -> dict[str, Any] | None:
    data = {key: row[key] for key in ["id", "name"]}

    # These are duplicates
    if data["name"] in (
        "Jackson Laboratory for Genomic Medicine",
        "Jackson Laboratory for Mammalian Genetics",
        "JAX Mice, Clinical, and Research Services",
        "University of Connecticut Storrs",
    ):
        return None

    return data


async def csv_to_new_institutions(
    client: aiohttp.ClientSession,
    institutions_url: str,
    data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    response = await client.get(institutions_url)
    pre_existing_institutions = {inst["id"] for inst in (await response.json())}
    new_institutions = (_parse_row(row) for row in data)
    new_institutions = (
        inst
        for inst in new_institutions
        if not (inst is None or inst["id"] in pre_existing_institutions)
    )

    return new_institutions
