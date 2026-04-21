import datetime
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from types import NoneType
from typing import Any
from uuid import UUID

import aiohttp
from pydantic.dataclasses import dataclass
from pydantic.main import BaseModel

NO_LIMIT_QUERY = {"q": json.dumps({"limit": 999_999})}


def to_snake_case(s: str):
    return s.lower().replace(" ", "_")


def str_to_float(s: str | int | float) -> float:
    if isinstance(s, float):
        return s
    if isinstance(s, int):
        return s

    f = float(s.replace(",", "").removesuffix("%"))

    if "%" in s:
        f = f / 100

    return f


def str_to_int(s: str) -> int:
    return int(str_to_float(s))


def str_to_bool(s: str | bool) -> bool | None:
    if isinstance(s, bool):
        return s

    return {"TRUE": True, "FALSE": False}.get(s)


# def _shitty_date_str_to_eastcoast_9am(date_str: str) -> datetime.datetime:
#     print(date_str)
#     i_cant_believe_this_is_the_format_month, day, year = (
#         date_str.split("-")[0].split("&")[0].split("/")
#     )
#     return datetime.datetime(
#         year=int(year),
#         month=int(i_cant_believe_this_is_the_format_month),
#         day=int(day),
#         hour=13,
#         tzinfo=datetime.UTC,
#     )


# def date_str_to_eastcoast_9am(date_str: str) -> datetime.datetime:
#     try:
#         date = datetime.date.fromisoformat(date_str)
#     except ValueError:
#         date = _shitty_date_str_to_eastcoast_9am(date_str)

#     return datetime.datetime(
#         year=date.year, month=date.month, day=date.day, hour=13, tzinfo=datetime.UTC
#     )


def property_id_map(
    property_name: str, data: list[dict[str, Any]], id_path: str = "id"
) -> dict[str, str]:
    map = {d[property_name]: d[id_path] for d in data}
    assert len(map) == len(data), f"property {property_name} is not unique"

    return map


def _rename_fields(
    data: Iterable[dict[str, Any]],
    field_renaming: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            field_renaming.get(field, to_snake_case(field)): value
            for field, value in row.items()
            if field is not None
        }
        for row in data
    ]


class JsonSpec(BaseModel):
    path: Path
    field_renaming: dict[str, str] = {}
    id_key: str = "readable_id"


def read_json_file(spec: JsonSpec) -> list[dict[str, Any]]:
    path = spec.path
    data = json.loads(path.read_bytes())

    return _rename_fields(data, spec.field_renaming)


async def get_project_name_id_map(
    client: aiohttp.ClientSession, labs_url: str
) -> dict[str, str]:
    async with client.get(labs_url, params=NO_LIMIT_QUERY) as resp:
        labs = await resp.json()
    labs = property_id_map("name", labs)
    labs = labs | {name.lower(): id for name, id in labs.items()}

    return labs


async def get_person_email_id_map(
    client: aiohttp.ClientSession, people_url: str
) -> dict[str, str]:
    async with client.get(people_url, params=NO_LIMIT_QUERY) as resp:
        people = await resp.json()
    people = property_id_map("email", people)
    people = people | {email.lower(): id for email, id in people.items()}

    return people


def _strip(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    elif isinstance(value, list):
        return [_strip(inner) for inner in value]
    elif isinstance(value, dict):
        return {_strip(key): _strip(val) for key, val in value.items()}
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, (int, float, bool, NoneType)):
        return value
    else:
        raise TypeError(f"cannot strip {type(value)}")


def strip_str_values(data: dict[str, Any], ahmed_id: str) -> dict[str, Any]:
    new_dict = {}
    for key, val in data.items():
        if isinstance(val, dict):
            new_dict[key] = strip_str_values(val, ahmed_id)
        else:
            new_dict[key] = _strip(val)

    # THIS IS A HACK BECAUSE WE DON'T KNOW HOW TO ENTER DATA
    if "preparer_ids" in new_dict and len(new_dict["preparer_ids"]) == 0:
        new_dict["preparer_ids"] = [ahmed_id]
    if "run_by" in new_dict and not new_dict["run_by"]:
        new_dict["run_by"] = ahmed_id
    if "measured_by" in new_dict and not new_dict["measured_by"]:
        new_dict["measured_by"] = ahmed_id

    return new_dict


async def write_error(
    request: dict[str, Any],
    response: aiohttp.ClientResponse,
    error_dir: Path,
    filename_generator: Callable[[dict[str, Any]], str] = lambda d: d.get(
        "readable_id", d.get("name", "ERROR")
    ),
):
    error_subdir = error_dir / str(filename_generator(request))
    error_subdir.mkdir(parents=True, exist_ok=True)

    filename = len(list(error_subdir.iterdir()))
    error_path = error_subdir / Path(f"{filename}.json")

    try:
        response_body = await response.json()
    except Exception:
        response_body = await response.text()

    to_write = {
        "request": request,
        "response": {
            "status": response.status,
            "extracted_body": response_body,
            "headers": {key: val for key, val in response.headers.items()},
        },
    }
    error_path.write_text(json.dumps(to_write))


@dataclass(kw_only=True, frozen=True)
class TenxAssaySpec:
    name: str
    sample_multiplexing: str
    chemistry_version: str
    chromium_chip: str
    library_types: tuple[str, ...]
