import asyncio
import logging
import uuid
from collections.abc import Generator
from copy import deepcopy
from typing import Any, Literal

import aiohttp

from utils import (
    NO_LIMIT_QUERY,
    TenxAssaySpec,
    get_person_email_id_map,
    property_id_map,
    str_to_bool,
    str_to_float,
)


def _parse_gem_pools(
    loadings: list[dict[str, Any]],
    suspensions: dict[str, str],
    suspension_pools: dict[str, str],
) -> dict[str, Any] | None:
    gems_readable_id = loadings[0]["gems_readable_id"]
    parsed_loadings = []

    for loading in loadings:
        parsed_loading = {}
        if suspension_readable_id := loading.get("suspension_readable_id"):
            parsed_loading["suspension_id"] = suspensions.get(suspension_readable_id)
        elif suspension_pool_readable_id := loading.get("suspension_pool_readable_id"):
            parsed_loading["suspension_pool_id"] = suspension_pools.get(
                suspension_pool_readable_id
            )

        if suspension_volume_loaded := loading.get("suspension_volume_loaded_(µl)"):
            parsed_loading["suspension_volume_loaded"] = {
                "value": str_to_float(suspension_volume_loaded),
                "unit": "microliter",
            }
        else:
            parsed_loading["suspension_volume_loaded"] = {
                "value": 0,
                "unit": "microliter",
            }

        if buffer_volume_loaded := parsed_loading.get("buffer_volume_loaded"):
            parsed_loading["buffer_volume_loaded"] = {
                "value": str_to_float(buffer_volume_loaded),
                "unit": "microliter",
            }
        else:
            parsed_loading["buffer_volume_loaded"] = {"value": 0, "unit": "microliter"}

        # This is thoroughly shit
        if str(loading["tag_id"]).lower().startswith("ob"):
            for barcode in loading["tag_id"].split("+"):
                this = deepcopy(parsed_loading)
                this["ocm_barcode_id"] = barcode.lower()
                if this not in parsed_loadings:
                    parsed_loadings.append(this)
        elif ocm_barcode_id := loading.get("ocm_barcode_id_(if_suspension_pool)"):
            for barcode in ocm_barcode_id.split("+"):
                this = deepcopy(parsed_loading)
                this["ocm_barcode_id"] = barcode.lower()
                if this not in parsed_loadings:
                    parsed_loadings.append(this)
        else:
            parsed_loadings.append(parsed_loading)

    n = len(loadings)

    if n == 1:
        parsed_loading = parsed_loadings[0]
        gem_pool = {"readable_id": gems_readable_id, "loading": parsed_loading}
        return gem_pool

    if n >= 1:
        gem_pool = {"readable_id": gems_readable_id, "loading": parsed_loadings}
        return gem_pool


def _gems_loading_succeeded(loadings: list[dict[str, Any]]):
    succeeded = True
    for loading in loadings:
        if clog := loading.get("clog/wetting_failure"):
            succeeded = succeeded and not clog

    return succeeded


def _gem_pool_plexy(
    gem_pool: dict[str, Any],
) -> Literal["standard", "on_chip_multiplexing"] | None:
    loading = gem_pool["loading"]

    if isinstance(loading, dict) and (
        loading.get("suspension_pool_id") or loading.get("suspension_id")
    ):
        return "standard"

    if isinstance(loading, list):
        if loading[0].get("ocm_barcode_id"):
            return "on_chip_multiplexing"
        return "standard"

    return None


def _plexy(
    gem_pools: list[dict[str, Any]],
) -> Literal["mixed", "standard", "on_chip_multiplexing"] | None:
    plexies = (_gem_pool_plexy(gem_pool) for gem_pool in gem_pools)
    plexies = {plexy for plexy in plexies if plexy}

    if len(plexies) == 0:
        return None
    if len(plexies) == 1:
        return plexies.pop()  # pyright: ignore
    else:
        return "mixed"


def _parse_chromium_run(
    chromium_run: list[dict[str, Any]],
    gems_loading: dict[str, list[dict[str, Any]]],
    people: dict[str, str],
    suspensions: dict[str, str],
    suspension_pools: dict[str, str],
    assays: dict[str, str],
) -> dict[str, Any] | None:
    data = {}

    data["readable_id"] = chromium_run[0]["chromium_run_readable_id"]

    run_at = chromium_run[0].get("date_chip_run")
    if run_at := run_at:
        data["run_at"] = run_at

    data["run_by"] = people.get(chromium_run[0]["chip_run_by"])
    data["assay_id"] = assays[chromium_run[0]["assay"]]
    data["succeeded"] = True

    gem_pools = []
    for gems_row in chromium_run:
        loadings = gems_loading.get(gems_row["readable_id"])
        if loadings is None:
            logging.warning(
                f"GEMs {gems_row['readable_id']} does not have a complete loading specified"
            )
            continue

        data["succeeded"] = data["succeeded"] and _gems_loading_succeeded(loadings)

        gem_pools.append(
            _parse_gem_pools(
                loadings,
                suspensions=suspensions,
                suspension_pools=suspension_pools,
            )
        )

    gem_pools = [g for g in gem_pools if g is not None]
    if not (gem_pools):
        return None

    data["plexy"] = _plexy(gem_pools)
    data["gem_pools"] = gem_pools

    return data


async def csv_to_chromium_runs(
    client: aiohttp.ClientSession,
    people_url: str,
    suspensions_url: str,
    suspension_pools_url: str,
    chromium_runs_url: str,
    tenx_assays_url: str,
    gem_pools_data: list[dict[str, Any]],
    gem_pools_loading_data: list[dict[str, Any]],
    assay_name_to_spec: dict[str, TenxAssaySpec],
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people_task = tg.create_task(get_person_email_id_map(client, people_url))
        suspensions_task = tg.create_task(
            client.get(suspensions_url, params=NO_LIMIT_QUERY)
        )
        suspension_pools_task = tg.create_task(
            client.get(suspension_pools_url, params=NO_LIMIT_QUERY)
        )
        pre_existing_chromium_runs_task = tg.create_task(
            client.get(chromium_runs_url, params=NO_LIMIT_QUERY)
        )
        tenx_assays_task = tg.create_task(
            client.get(tenx_assays_url, params=NO_LIMIT_QUERY)
        )

    people = people_task.result()
    suspensions = await suspensions_task.result().json()
    suspension_pools = await suspension_pools_task.result().json()
    pre_existing_chromium_runs = await pre_existing_chromium_runs_task.result().json()
    tenx_assays = await tenx_assays_task.result().json()

    tenx_assays = {TenxAssaySpec(**a): a["id"] for a in tenx_assays}
    tenx_assays = {
        assay_name: tenx_assays[assay_spec]
        for assay_name, assay_spec in assay_name_to_spec.items()
    }

    suspensions = property_id_map("readable_id", suspensions)
    suspension_pools = property_id_map("readable_id", suspension_pools)
    pre_existing_chromium_runs = property_id_map(
        "readable_id", pre_existing_chromium_runs
    )

    chromium_runs = {
        gems_row["chromium_run_readable_id"]: [] for gems_row in gem_pools_data
    }
    for gems_row in gem_pools_data:
        chromium_runs[gems_row["chromium_run_readable_id"]].append(gems_row)

    gems_loading = {
        gems_loading_row["gems_readable_id"]: []
        for gems_loading_row in gem_pools_loading_data
    }
    for gems_loading_row in gem_pools_loading_data:
        gems_loading[gems_loading_row["gems_readable_id"]].append(gems_loading_row)

    chromium_runs = (
        _parse_chromium_run(
            chromium_run,
            gems_loading,
            people,
            suspensions,
            suspension_pools,
            tenx_assays,
        )
        for chromium_run in chromium_runs.values()
        if chromium_run
    )

    return (
        run
        for run in chromium_runs
        if (run is not None and run["readable_id"] not in pre_existing_chromium_runs)
    )
