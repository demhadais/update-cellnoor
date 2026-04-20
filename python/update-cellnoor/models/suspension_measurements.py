from datetime import datetime, timedelta
from typing import Any, Literal

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    get_person_email_id_map,
    str_to_float,
    to_snake_case,
)


def _parse_concentration(
    row: dict[str, Any],
    value_key: str,
    numerator: str | None = None,
    counting_method: str | None = None,
) -> dict[str, Any] | None:
    COUNTING_METHODS = {"aopi": "acridine_orange_propidium_iodide"}

    parsed_concentration: dict[str, Any] = {"quantity": "concentration"}
    if value := row.get(value_key):
        parsed_concentration["value"] = int(str_to_float(value))
    else:
        return None

    if counting_method is not None:
        counting_method = to_snake_case(counting_method)
        parsed_concentration["counting_method"] = COUNTING_METHODS.get(
            counting_method, counting_method
        )

    if numerator is None:
        parsed_concentration["numerator_unit"] = to_snake_case(
            row["biological_material"]
        )

    return parsed_concentration


def _parse_volume(
    row: dict[str, Any],
    value_key: str,
) -> dict[str, Any] | None:
    if value := row.get(value_key):
        value = str_to_float(value)
    else:
        return None

    return {
        "quantity": "volume",
        "value": value,
    }


def _parse_viability(
    row: dict[str, Any],
    value_key: str,
) -> dict[str, Any] | None:
    if value := row.get(value_key):
        # Divide by 100 because these values are formatted in a reasonable way (without the percent-sign) so they won't automatically be converted to a decimal inside str_to_float
        value = str_to_float(value) / 100
    else:
        return None

    return {
        "quantity": "viability",
        "value": value,
    }


def _parse_cell_or_nucleus_diameter(
    row: dict[str, Any],
    value_key: str,
    suspension_content: Literal["cell", "nucleus"] | None = None,
) -> dict[str, Any] | None:
    if value := row.get(value_key):
        value = str_to_float(value)
    else:
        return None

    if suspension_content is None:
        suspension_content = to_snake_case(row["biological_material"])  # pyright: ignore[reportAssignmentType]

    return {
        "quantity": "mean_diameter",
        "value": value,
        "object": suspension_content,
    }


def _extract_measurements_from_row(
    row: dict[str, Any],
    people: dict[str, str],
    suspension: dict[str, Any],
) -> list[dict[str, Any]]:
    measurements = []
    parent_specimen: dict[str, Any] = suspension["parent_specimen"]

    customer_measured_at = parent_specimen["received_at"]

    if date_created := suspension["created_at"]:
        first_scbl_measurement_time = date_created
    else:
        first_scbl_measurement_time = row["date_experiment_begun"]

    second_scbl_measurement_time = first_scbl_measurement_time
    post_hybridization_measurement_time = datetime.fromisoformat(
        second_scbl_measurement_time
    ) + timedelta(days=1)

    measured_by_for_customer_measurement = parent_specimen["submitted_by"]
    measured_by_for_scbl_measurement = people.get(row.get("preparer_1_email"))

    concentrations = [
        (
            "customer_cell/nucleus_concentration_(cell-nucleus/ml)",
            None,
            customer_measured_at,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(cell-nucleus/ml)",
            row.get("counting_method"),
            first_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(post-adjustment)_(cell-nucleus/ml)",
            row.get("counting_method"),
            second_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "post-hybridization_cell/nucleus_concentration_(cell-nucleus/ml)",
            row.get("counting_method"),
            post_hybridization_measurement_time,
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for (
        key,
        counting_method,
        measured_at,
        measured_by,
        is_post_probe_hybridization,
    ) in concentrations:
        if measurement_data := _parse_concentration(
            row,
            value_key=key,
            counting_method=counting_method,
        ):
            measurement = {
                "measured_by": measured_by,
                "measured_at": measured_at,
                "data": measurement_data
                | {
                    "denominator_unit": "milliliter",
                    "post_hybridization": is_post_probe_hybridization,
                },
            }
            measurements.append(measurement)

    volumes = [
        (
            "customer_volume_(µl)",
            customer_measured_at,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_volume_(µl)",
            first_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_volume_(post-adjustment)_(µl)",
            second_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "post-hybridization_volume_(µl)",
            post_hybridization_measurement_time,
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, measured_at, measured_by, is_post_probe_hybridization in volumes:
        if measurement_data := _parse_volume(row, value_key=key):
            measurement = {
                "measured_by": measured_by,
                "measured_at": measured_at,
                "data": measurement_data
                | {
                    "post_hybridization": is_post_probe_hybridization,
                    "unit": "microliter",
                },
            }
            measurements.append(measurement)

    viabilities = [
        (
            "customer_cell_viability_(%)",
            customer_measured_at,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(%)",
            first_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(post-adjustment)_(%)",
            second_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
    ]
    for key, measured_at, measured_by, is_post_probe_hybridization in viabilities:
        if measurement_data := _parse_viability(row, value_key=key):
            measurement = {
                "measured_by": measured_by,
                "measured_at": measured_at,
                "data": measurement_data
                | {
                    "post_hybridization": is_post_probe_hybridization,
                },
            }
            measurements.append(measurement)

    diameters = [
        (
            "scbl_average_cell/nucleus_diameter_(µm)",
            first_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_average_cell/nucleus_diameter_(post-adjustment)_(µm)",
            second_scbl_measurement_time,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_post-hybridization_average_cell/nucleus_diameter_(µm)",
            post_hybridization_measurement_time,
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, measured_at, measured_by, is_post_probe_hybridization in diameters:
        if measurement_data := _parse_cell_or_nucleus_diameter(
            row,
            value_key=key,
        ):
            measurement = {
                "measured_by": measured_by,
                "measured_at": measured_at,
                "data": measurement_data
                | {
                    "post_hybridization": is_post_probe_hybridization,
                    "unit": "micrometer",
                },
            }
            measurements.append(measurement)

    return measurements


async def csv_to_suspension_measurements(
    people_url: str,
    suspensions_url: str,
    data: list[dict[str, Any]],
    client: aiohttp.ClientSession,
) -> list[tuple[str, list[dict[str, Any]]]]:
    # I could not care less about making this faster using actual async features. I hate this language and this project
    people = await get_person_email_id_map(client, people_url)
    suspensions_resp = await client.get(suspensions_url, params=NO_LIMIT_QUERY)
    suspensions = await suspensions_resp.json()
    suspensions = {suspension["readable_id"]: suspension for suspension in suspensions}
    rows_by_readable_id = {row["readable_id"]: row for row in data}

    relevant_rows_i_hate_python_and_spreadsheets = [
        (rows_by_readable_id[readable_id], suspension)
        for readable_id, suspension in suspensions.items()
    ]

    measurements = []

    for row, suspension in relevant_rows_i_hate_python_and_spreadsheets:
        resp = await client.get(f"{suspensions_url}/{suspension['id']}")
        suspension = await resp.json()

        measurement_set = _extract_measurements_from_row(
            row, people=people, suspension=suspension
        )

        if not measurement_set:
            continue

        measurements.append((suspension["id"], measurement_set))

    return measurements
