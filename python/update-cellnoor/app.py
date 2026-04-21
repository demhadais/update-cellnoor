import asyncio
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, override

import aiohttp
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from models.cdna import csv_to_new_cdna
from models.chromium_datasets import post_chromium_datasets, upload_dataset_files
from models.chromium_runs import csv_to_chromium_runs
from models.institutions import (
    csv_to_new_institutions,
)
from models.libraries import csv_to_new_libraries
from models.people import csv_to_new_people
from models.projects import csv_to_new_projects
from models.specimen_measurements import csv_to_new_specimen_measurements
from models.specimens import csv_to_new_specimens
from models.suspension_measurements import csv_to_suspension_measurements
from models.suspension_pools import csvs_to_new_suspension_pools
from models.suspensions import csv_to_new_suspensions
from utils import JsonSpec, TenxAssaySpec, read_json_file, strip_str_values, write_error

UPDATE_CELLNOOR = "update-cellnoor"


async def _post_many(
    client: aiohttp.ClientSession,
    url: list[str] | str,
    data: Iterable[dict[str, Any]],
    ahmed_id: str,
) -> list[tuple[dict[str, Any], aiohttp.ClientResponse]]:
    stripped_string_data = (strip_str_values(d, ahmed_id) for d in data)
    responses: list[tuple[dict[str, Any], asyncio.Task[aiohttp.ClientResponse]]] = []
    async with asyncio.TaskGroup() as tg:
        if isinstance(url, str):
            for request_body in stripped_string_data:
                task = tg.create_task(client.post(url, json=request_body))
                responses.append((request_body, task))
        else:
            for u, request_body in zip(url, stripped_string_data, strict=True):
                task = tg.create_task(client.post(u, json=request_body))
                responses.append((request_body, task))

    return [(request_body, task.result()) for request_body, task in responses]


async def _write_errors(
    request_response_pairs: list[tuple[dict[str, Any], aiohttp.ClientResponse]],
    error_dir: Path,
    filename_generator: Callable[[dict[str, Any]], str] = lambda d: d.get(
        "readable_id", d.get("name", "ERROR")
    ),
):
    for req, resp in (
        (req, resp) for req, resp in request_response_pairs if not resp.ok
    ):
        await write_error(
            request=req,
            response=resp,
            error_dir=error_dir,
            filename_generator=filename_generator,
        )


async def _update_cellnoor_api(settings: "Settings"):
    connector = aiohttp.TCPConnector(ssl=not settings.accept_invalid_certificates)
    client = aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {settings.api_token}"},
        connector=connector,
    )

    try:
        await _update_cellnoor_api_inner(client, settings)
    except Exception as e:
        raise e
    finally:
        await client.close()


async def _update_cellnoor_api_inner(
    client: aiohttp.ClientSession, settings: "Settings"
):
    errors_dir = settings.errors_dir

    institution_url = f"{settings.api_base_url}/institutions"
    if institutions := settings.institutions:
        data = read_json_file(institutions)
        new_institutions = await csv_to_new_institutions(
            client,
            institution_url,
            data,
        )

        responses = await _post_many(client, institution_url, new_institutions, "")
        await _write_errors(responses, errors_dir)

    people_url = f"{settings.api_base_url}/people"
    if people := settings.people:
        data = read_json_file(people)
        new_people = await csv_to_new_people(
            client,
            institution_url=institution_url,
            people_url=people_url,
            data=data,
        )
        responses = await _post_many(client, people_url, new_people, "")
        await _write_errors(
            responses,
            errors_dir,
            lambda pers: (
                pers["email"].replace("@", "at")
                if pers["email"] is not None
                else "unknown-email"
            ),
        )

    async with client.get(
        f"{settings.api_base_url}/people",
        params={
            "q": json.dumps(
                {
                    "filter": {
                        "emails": ["ahmed.said@jax.org", "scamplers-test@outlook.com"]
                    }
                }
            )
        },
    ) as response:
        data: list[dict[str, Any]] = await response.json()
        ahmed_id: str = data[0]["id"]

    project_url = f"{settings.api_base_url}/projects"
    if labs := settings.projects:
        data = read_json_file(labs)
        new_projects = await csv_to_new_projects(
            client,
            project_url=project_url,
            data=data,
        )
        responses = await _post_many(client, project_url, new_projects, ahmed_id)
        await _write_errors(
            responses, errors_dir, lambda project: project["name"].replace(" ", "")
        )

    specimen_url = f"{settings.api_base_url}/specimens"
    if specimens := settings.specimens:
        data = read_json_file(specimens)
        new_specimens = await csv_to_new_specimens(
            client,
            people_url=people_url,
            project_url=project_url,
            specimen_url=specimen_url,
            data=data,
        )
        request_response_pairs = await _post_many(
            client, specimen_url, new_specimens, ahmed_id
        )

        await _write_errors(request_response_pairs, errors_dir)

    def specimen_measurement_url_creator(specimen_id: str):
        return f"{specimen_url}/{specimen_id}/measurements"

    if specimen_measurements := settings.specimen_measurements:
        data = read_json_file(specimen_measurements)
        new_specimen_measurements = await csv_to_new_specimen_measurements(
            client,
            specimen_url=specimen_url,
            people_url=people_url,
            specimen_measurement_url_creator=specimen_measurement_url_creator,
            data=data,
        )
        new_specimen_measurements = list(new_specimen_measurements)

        urls = [
            f"{specimen_url}/{specimen_id}/measurements"
            for specimen_id, _ in new_specimen_measurements
        ]
        data = [measurement for _, measurement in new_specimen_measurements]

        request_response_pairs = await _post_many(client, urls, data, ahmed_id)
        await _write_errors(
            request_response_pairs,
            errors_dir,
            filename_generator=lambda _: "specimen-measurements",
        )

    suspensions_url = f"{settings.api_base_url}/suspensions"
    multiplexing_tags_url = f"{settings.api_base_url}/multiplexing-tags"
    if suspensions := settings.suspensions:
        data = read_json_file(suspensions)
        new_suspensions = await csv_to_new_suspensions(
            client,
            people_url=people_url,
            specimens_url=specimen_url,
            multiplexing_tags_url=multiplexing_tags_url,
            suspensions_url=suspensions_url,
            data=data,
            id_key=suspensions.id_key,
        )
        request_response_pairs = await _post_many(
            client, f"{suspensions_url}", new_suspensions, ahmed_id
        )
        await _write_errors(request_response_pairs, errors_dir)

        new_suspension_measurements = await csv_to_suspension_measurements(
            people_url=people_url,
            suspensions_url=suspensions_url,
            data=data,
            client=client,
        )

        urls = [
            f"{suspensions_url}/{suspension_id}/measurements"
            for suspension_id, measurement_set in new_suspension_measurements
            for _ in measurement_set
        ]
        data = [
            measurement
            for _, measurement_set in new_suspension_measurements
            for measurement in measurement_set
        ]

        request_response_pairs = await _post_many(client, urls, data, ahmed_id)
        await _write_errors(
            request_response_pairs,
            errors_dir,
            filename_generator=lambda _: "suspension-measurements",
        )

    if settings.suspension_pools and settings.suspensions is None:
        raise ValueError("cannot specify suspension pools without suspensions")

    suspension_pools_url = f"{settings.api_base_url}/suspension-pools"
    if settings.suspension_pools:
        data = read_json_file(settings.suspension_pools)
        new_suspension_pools = await csvs_to_new_suspension_pools(
            client,
            people_url=people_url,
            suspension_pool_url=suspension_pools_url,
            suspensions_url=suspensions_url,
            multiplexing_tags_url=multiplexing_tags_url,
            suspension_pool_data=data,
            suspension_csv_data=read_json_file(settings.suspensions),  # pyright: ignore[reportArgumentType]
        )

        request_response_pairs = await _post_many(
            client, suspension_pools_url, new_suspension_pools, ahmed_id
        )
        await _write_errors(
            request_response_pairs,
            errors_dir,
        )

    if settings.gems is None != settings.gems_suspensions is None:
        raise ValueError("cannot specify GEMs CSV without GEMs-suspensions")

    tenx_assays_url = f"{settings.api_base_url}/10x-assays"
    chromium_runs_url = f"{settings.api_base_url}/chromium-runs"
    if (gems := settings.gems) and (gems_suspensions := settings.gems_suspensions):
        gems = read_json_file(gems)
        gems_suspensions = read_json_file(gems_suspensions)
        new_chromium_runs = await csv_to_chromium_runs(
            client,
            people_url=people_url,
            suspensions_url=suspensions_url,
            suspension_pools_url=suspension_pools_url,
            tenx_assays_url=tenx_assays_url,
            chromium_runs_url=chromium_runs_url,
            assay_name_to_spec=settings.assay_map,
            gem_pools_data=gems,
            gem_pools_loading_data=gems_suspensions,
        )

        request_response_pairs = await _post_many(
            client, chromium_runs_url, new_chromium_runs, ahmed_id
        )
        await _write_errors(request_response_pairs, settings.errors_dir)

    cdna_url = f"{settings.api_base_url}/cdna"
    if cdna := settings.cdna:
        data = read_json_file(cdna)
        new_cdna = await csv_to_new_cdna(
            client,
            cdna_url=cdna_url,
            gem_pool_url=f"{settings.api_base_url}/gem-pools",
            people_url=people_url,
            data=data,
            id_key=settings.cdna.id_key,
        )

        request_response_pairs = await _post_many(client, cdna_url, new_cdna, ahmed_id)
        await _write_errors(request_response_pairs, settings.errors_dir)

    libraries_url = f"{settings.api_base_url}/libraries"
    if libraries := settings.libraries:
        data = read_json_file(libraries)
        new_libraries = await csv_to_new_libraries(
            client,
            cdna_url=cdna_url,
            libraries_url=libraries_url,
            people_url=people_url,
            data=data,
        )

        request_response_pairs = await _post_many(
            client, libraries_url, new_libraries, ahmed_id
        )
        await _write_errors(request_response_pairs, settings.errors_dir)

    # sequencing_runs_url = f"{settings.api_base_url}/sequencing-runs"
    # if sequencing_submissions := settings.sequencing_submissions:
    #     data = read_csv(sequencing_submissions)
    #     new_sequencing_runs = await csv_to_new_sequencing_runs(data=data)

    #     request_response_pairs = await _post_many(
    #         client,
    #         sequencing_runs_url,
    #         new_sequencing_runs,
    #     )
    #     await _write_errors(request_response_pairs, settings.errors_dir)

    chromium_datasets_url = f"{settings.api_base_url}/chromium-datasets"
    if dataset_dirs := settings.dataset_dirs:
        await post_chromium_datasets(
            client,
            chromium_datasets_url,
            libraries_url,
            dataset_dirs,
            settings.errors_dir,
        )

        await upload_dataset_files(
            client, chromium_datasets_url, dataset_dirs, errors_dir
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=UPDATE_CELLNOOR.upper(),
        cli_kebab_case=True,
    )

    config_path: Path = (
        Path.home() / ".config" / UPDATE_CELLNOOR / f"{UPDATE_CELLNOOR}.toml"
    )
    api_base_url: str
    api_token: str
    accept_invalid_certificates: bool = False
    institutions: JsonSpec | None = None
    people: JsonSpec | None = None
    projects: JsonSpec | None = None
    specimens: JsonSpec | None = None
    specimen_measurements: JsonSpec | None = None
    suspensions: JsonSpec | None = None
    suspension_pools: JsonSpec | None = None
    gems: JsonSpec | None = None
    gems_suspensions: JsonSpec | None = None
    cdna: JsonSpec | None = None
    libraries: JsonSpec | None = None
    sequencing_submissions: JsonSpec | None = None
    dataset_dirs: CliPositionalArg[list[Path]] = []
    assay_map: dict[str, TenxAssaySpec]
    dry_run: bool = False
    print_requests: bool = False
    save_requests: Path | None = None
    log_errors: bool = True
    errors_dir: Path = Path(".errors")

    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            TomlConfigSettingsSource(
                settings_cls,
                toml_file=cls.model_fields["config_path"].default,
            ),
        )

    async def cli_cmd(self):
        await _update_cellnoor_api(self)
