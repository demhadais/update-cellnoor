# TODO: YOU ARE ALLOWED TO USE VARIABLES
import asyncio
import uuid
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    get_person_email_id_map,
    get_project_name_id_map,
    to_snake_case,
)

SUSPENSION_FIXATIVES = {
    "Formaldehyde-derivative fixed": "formaldehyde_derivative",
    "DSP-fixed": "dithiobis_succinimidylpropionate",
    "Scale DSP-Fixed": "dithiobis_succinimidylpropionate",
}


def _parse_row(
    row: dict[str, Any],
    projects: dict[str, str],
    people: dict[str, str],
) -> dict[str, Any] | None:
    data = {
        simple_key: row[simple_key] for simple_key in ["name", "readable_id", "tissue"]
    }

    data["project_id"] = projects.get(row["lab_name"], str(uuid.uuid7()))

    if submitter_email := row["submitter_email"]:
        data["submitted_by"] = people[submitter_email.lower()]

    if date_received := row.get("date_received"):
        data["received_at"] = date_received

    if row["returner_email"] not in (None, "0", 0, ""):
        data["returned_by"] = people[row["returner_email"]]

    if date_returned := row.get("date_returned"):
        data["returned_at"] = date_returned

    if (
        data.get("received_at") == data.get("returned_at")
        and data.get("returned_at") is not None
    ):
        data["returned_at"] = (
            datetime.fromisoformat(data["returned_at"]) + timedelta(hours=1)
        ).isoformat()

    if row["species"] == "Homo sapiens + Mus musculus (PDX)":
        data["species"] = "homo_sapiens"
        data["host_species"] = "mus_musculus"
    elif row["species"]:
        data["species"] = to_snake_case(row["species"])

    data["additional_data"] = {
        key: row[key]
        for key in [
            "condition",
            "storage_buffer",
            "notes",
        ]
        if row.get(key) is not None
    }
    if len(data["additional_data"]) == 0:
        del data["additional_data"]

    preliminary_em = row.get("embedding_matrix")
    if preliminary_em is not None:
        data["embedded_in"] = {
            "CMC": "carboxymethyl_cellulose",
            "OCT": "optimal_cutting_temperature_compound",
        }.get(preliminary_em)
        if data["embedded_in"] is None:
            data["embedded_in"] = to_snake_case(preliminary_em)

    match (row["type"], row["preservation_method"]):
        # TODO: remove this
        # case ("Nucleus Suspension", "Flash-frozen"):
        #     data["type"] = "tissue"
        #     data["preservation_state"] = "fresh"
        case ("Block" | "Curl", preservation) if preservation != "Fresh":
            preservation_to_fixative = {
                "Formaldehyde-derivative fixed": "formaldehyde_derivative",
                "Formaldehyde-derivative fixed & flash-frozen (blocks only)": "formaldehyde_derivative",
                "Flash-frozen": None,
            }
            data["fixative"], data["type"] = (
                preservation_to_fixative[preservation],
                "block",
            )
        case ("Tissue", "Cryopreserved (controlled-rate freezing)"):
            data["type"] = "tissue"
            data["thermal_preservation_method"] = "controlled_rate_freezing"
            data["preservation_state"] = "thermally_preserved"
        case ("Tissue", "DSP-fixed" | "Scale DSP-Fixed"):
            data["type"] = "tissue"
            data["fixative"] = "dithiobis_succinimidylpropionate"
            data["preservation_state"] = "fixed"
        case ("Tissue", "Fresh" | None):
            data["type"] = "tissue"
            data["preservation_state"] = "fresh"
        case ("Tissue", "Flash-frozen"):
            data["type"] = "tissue"
            data["thermal_preservation_method"] = "flash_freezing"
            data["preservation_state"] = "thermally_preserved"
        case (
            "Cell Suspension" | "Nucleus Suspension",
            "Cryopreserved (controlled-rate freezing)",
        ):
            data["type"] = "suspension"
            data["thermal_preservation_method"] = "controlled_rate_freezing"
            data["preservation_state"] = "thermally_preserved"
        case ("Cell Suspension" | "Nucleus Suspension", "Fresh" | None):
            data["type"] = "suspension"
            data["preservation_state"] = "fresh"
        case ("Cell Pellet", "Flash-frozen"):
            data["type"] = "cell_pellet"
            data["preservation_state"] = "thermally_preserved"
            data["thermal_preservation_method"] = "flash_freezing"
        case (
            "Cell Suspension" | "Nucleus Suspension",
            preservation,
        ) if preservation in SUSPENSION_FIXATIVES:
            data["type"] = "suspension"
            data["preservation_state"] = "fixed"
            data["fixative"] = SUSPENSION_FIXATIVES.get(preservation)
        case ("RNA Extract", _):
            data["type"] = "rna_extract"
        case (ty, preservation_method):
            data["type"] = ty
            data["preservation_state"] = preservation_method

    return data


async def csv_to_new_specimens(
    client: aiohttp.ClientSession,
    people_url: str,
    project_url: str,
    specimen_url: str,
    data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        projects = tg.create_task(get_project_name_id_map(client, project_url))

    people = people.result()
    projects = projects.result()

    new_specimens = (_parse_row(row, projects=projects, people=people) for row in data)

    resp = await client.get(specimen_url, params=NO_LIMIT_QUERY)
    pre_existing_specimens = {s["readable_id"] for s in await resp.json()}

    new_specimens = (
        spec
        for spec in new_specimens
        if not (spec is None or spec["readable_id"] in pre_existing_specimens)
    )

    return new_specimens
