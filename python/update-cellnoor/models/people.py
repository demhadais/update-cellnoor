from collections.abc import Generator
from typing import Any

import aiohttp

from utils import NO_LIMIT_QUERY, property_id_map


def _parse_row(
    row: dict[str, Any], institution_domains: dict[str, str]
) -> dict[str, Any] | None:
    required_keys = {"name", "email"}
    data = {key: row[key] for key in required_keys if key in row}

    email_domain = row["email"].split("@")[-1] if row["email"] is not None else ""
    data["institution_id"] = institution_domains.get(email_domain)

    microsoft_entra_oid_key = "microsoft_entra_oid"
    if microsoft_entra_oid := row[microsoft_entra_oid_key]:
        data[microsoft_entra_oid_key] = microsoft_entra_oid

    return data


async def _email_domain_institution_map(
    client: aiohttp.ClientSession, institution_url: str
) -> dict[str, str]:
    async with client.get(institution_url) as resp:
        institutions = await resp.json()
    institution_names = property_id_map("name", institutions)

    institution_domains = {
        "Banner MD Anderson Cancer Center": "mdanderson.org",
        "Cold Spring Harbor Laboratory": "cshl.edu",
        "Houston Methodist": "houstonmethodist.org",
        "Jackson Laboratory": "jax.org",
        "University of Connecticut": "uconn.edu",
        "University of Connecticut Health Center": "uchc.edu",
        "Connecticut Children’s Research Institute": "connecticutchildrens.org",
        "National Institutes of Health": "nih.gov",
        "Yale University": "yale.edu",
        "Pennsylvania State University": "psu.edu",
        "Purdue University": "purdue.edu",
    }

    institution_domains = {
        institution_domains[institution_name]: institution_id
        for institution_name, institution_id in institution_names.items()
    }

    return institution_domains


async def csv_to_new_people(
    client: aiohttp.ClientSession,
    institution_url: str,
    people_url: str,
    data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    institution_domains = await _email_domain_institution_map(client, institution_url)
    new_people = (_parse_row(row, institution_domains) for row in data)
    async with client.get(people_url, params=NO_LIMIT_QUERY) as resp:
        people_json = await resp.json()
    pre_existing_people = {p["email"] for p in people_json}
    pre_existing_people = pre_existing_people | {
        email.lower() for email in pre_existing_people if email is not None
    }

    new_people = (
        p for p in new_people if not (p is None or p["email"] in pre_existing_people)
    )

    return new_people
