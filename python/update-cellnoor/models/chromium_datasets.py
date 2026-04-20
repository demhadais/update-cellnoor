import asyncio
import itertools
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from compression import zstd
from compression.zstd import CompressionParameter

from copy_chromium_datasets import (
    get_cellranger_output_files,
    get_cmdline_file,
    get_pipeline_metadata_file,
)
from utils import NO_LIMIT_QUERY, property_id_map, write_error

# TODO: this module could easily be made more performant with a clever redesign. If we created some kind of queue where files were compressed in one thread and handed off to another for sending when a batch is complete, then we can separate out the work. Maybe


def _get_delivered_at(dataset_directory: Path) -> str:
    pipeline_metadata = get_pipeline_metadata_file(dataset_directory).read_bytes()
    pipeline_metadata = json.loads(pipeline_metadata)
    delivered_at = pipeline_metadata["metadata_generated_date"]
    delivered_at = datetime.fromisoformat(delivered_at).replace(tzinfo=UTC)

    return delivered_at.isoformat()


async def _post_dataset(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    path: Path,
    libraries: dict[str, str],
    error_dir: Path,
):
    library_readable_ids1 = re.findall(r"25E\d+-L\d+", path.name)
    library_readable_ids2 = re.findall(r"26CH\d+-L\d+", path.name)

    try:
        library_ids = [
            libraries[id] for id in library_readable_ids1 + library_readable_ids2
        ]
    except KeyError:
        return None

    data: dict[str, Any] = {"name": path.name}
    data["delivered_at"] = _get_delivered_at(path)
    data["library_ids"] = library_ids
    data["cmdline"] = " ".join(get_cmdline_file(path).read_text().split()[0:2])

    # This specific dataset was slightly weird
    if data["name"] == "25E50-L4_WIBJ2" or data["name"] == "25E50-L3_WIBJ2":
        data["cmdline"] = "cellranger multi"

    response = await client.post(chromium_datasets_url, json=data)
    if not response.ok:
        await write_error(request=data, response=response, error_dir=error_dir)
        return


_CONTENT_TYPES = {"html": "text/html", "json": "application/json", "csv": "text/csv"}

_ZSTD_OPTIONS = {
    CompressionParameter.compression_level: 22,
    # 2^22 = roughly 4MB. Browsers seem to hate anything over 8 MB for the window size, so we'll just do a power-of-two less than that :)
    CompressionParameter.window_log: 22,
}


async def _upload_files_for_one_dataset(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    dataset_id: str,
    path: Path,
    error_dir: Path,
):
    dataset_fileset = get_cellranger_output_files(path)

    def _read_and_compress():
        return [
            (
                filename,
                zstd.compress(path.read_bytes(), options=_ZSTD_OPTIONS),  # pyright: ignore[reportArgumentType]
            )
            if filename.endswith(".html")
            else (filename, path.read_bytes())
            for filename, path in dataset_fileset.files
        ]

    files = await asyncio.to_thread(_read_and_compress)
    # NEVER CHANGE THE FOLLOWING CODE. Trying to do this using aiohttp's other facilities doesn't work :)
    file_uploads = aiohttp.FormData(quote_fields=False, default_to_multipart=True)
    for filename, file_content in files:
        file_uploads.add_field(
            filename,
            file_content,
            content_type=_CONTENT_TYPES[filename.split(".")[-1]],
            filename=filename,
        )

    response = await client.post(
        f"{chromium_datasets_url}/{dataset_id}/raw-files",
        data=file_uploads,
        headers={"Content-Encoding": "zstd"},
    )

    if not response.ok:
        await write_error(
            request={
                "action": "uploaded files",
                "name": dataset_id,
                "file_paths": [fname for fname, _ in dataset_fileset.files],
            },
            response=response,
            error_dir=error_dir,
        )


async def upload_dataset_files(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    dataset_dirs: list[Path],
    errors_dir: Path,
):
    response = await client.get(chromium_datasets_url, params=NO_LIMIT_QUERY)
    pre_existing_datasets = await response.json()
    dataset_dir_map = {d.name: d for d in dataset_dirs}

    # The server can really only handle 4 at a time (I wrote the server)
    for dataset_batch in itertools.batched(pre_existing_datasets, 4):
        tasks = []
        async with asyncio.TaskGroup() as tg:
            for dataset in dataset_batch:
                # We can just skip datasets with files
                if dataset["links"]["raw_files"]:
                    continue

                dataset_dir = dataset_dir_map[dataset["name"]]

                task = tg.create_task(
                    _upload_files_for_one_dataset(
                        client,
                        chromium_datasets_url=chromium_datasets_url,
                        dataset_id=dataset["id"],
                        path=dataset_dir,
                        error_dir=errors_dir,
                    )
                )
                tasks.append(task)


async def post_chromium_datasets(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    libraries_url: str,
    dataset_dirs: list[Path],
    errors_dir: Path,
):
    async with asyncio.TaskGroup() as tg:
        libraries = tg.create_task(client.get(libraries_url, params=NO_LIMIT_QUERY))
        pre_existing_datasets = tg.create_task(
            client.get(chromium_datasets_url, params=NO_LIMIT_QUERY)
        )

    libraries = await libraries.result().json()
    libraries = property_id_map("readable_id", libraries)

    pre_existing_datasets = await pre_existing_datasets.result().json()
    pre_existing_datasets = property_id_map("name", pre_existing_datasets)

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for path in dataset_dirs:
            if path.name in pre_existing_datasets:
                continue

            task = tg.create_task(
                _post_dataset(
                    client, chromium_datasets_url, path, libraries, errors_dir
                )
            )
            tasks.append(task)
