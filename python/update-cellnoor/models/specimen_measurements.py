import asyncio
from collections.abc import Callable, Generator
from typing import Any

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    property_id_map,
    str_to_float,
)


def _parse_specimen_measurement_row(
    row: dict[str, Any],
    people: dict[str, Any],
    specimens: dict[str, dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]] | None:

    specimen = specimens.get(row["specimen_readable_id"])
    if specimen is None:
        return None

    measurements = []
    for column_name, measurement_quantity in [("rin", "RIN"), ("dv200", "DV200")]:
        measurement = {key: row[key] for key in ["instrument_name"] if key in row}
        if measured_by := people.get(row.get("measured_by")):
            measurement["measured_by"] = measured_by

        if measured_at := row.get("date_measured"):
            measurement["measured_at"] = measured_at
        else:
            measurement["measured_at"] = specimen["received_at"]

        if value := row.get(column_name):
            if value != " ":
                measurement["data"] = {
                    "quantity": measurement_quantity,
                    "value": str_to_float(value),
                }
                measurements.append(measurement)

    if not measurements:
        return None

    return (specimen["id"], measurements)


async def _get_pre_existing_measurements(
    client: aiohttp.ClientSession,
    specimen_ids: list[str],
    specimen_measurement_url_creator: Callable[[str], str],
) -> list[dict[str, Any]]:
    tasks = []

    async with asyncio.TaskGroup() as tg:
        for specimen_id in specimen_ids:
            url = specimen_measurement_url_creator(specimen_id)
            tasks.append(tg.create_task(client.get(url)))

    return [m for task in tasks for m in await task.result().json()]


async def csv_to_new_specimen_measurements(
    client: aiohttp.ClientSession,
    specimen_url: str,
    people_url: str,
    specimen_measurement_url_creator: Callable[[str], str],
    data: list[dict[str, Any]],
) -> Generator[tuple[str, dict[str, Any]]]:
    specimens = await (await client.get(specimen_url, params=NO_LIMIT_QUERY)).json()
    specimen_id_map = {spec["readable_id"]: spec for spec in specimens}

    if len(specimen_id_map) != len(specimens):
        raise ValueError("specimen readable IDs are not unique")

    people = await (await client.get(people_url, params=NO_LIMIT_QUERY)).json()
    people = property_id_map("email", people)

    pre_existing_measurements = await _get_pre_existing_measurements(
        client, [sp["id"] for sp in specimens], specimen_measurement_url_creator
    )

    for m in pre_existing_measurements:
        # delete the measurement ID so that the parsed measurement row can compare to the pre-existing measurments
        del m["id"]

    measurements = (
        _parse_specimen_measurement_row(
            row,
            people=people,
            specimens=specimen_id_map,
        )
        for row in data
    )

    measurements = (m for m in measurements if m is not None)

    return (
        (specimen_id, measurement)
        for specimen_id, measurement_set in measurements
        for measurement in measurement_set
        if measurement not in pre_existing_measurements
    )
