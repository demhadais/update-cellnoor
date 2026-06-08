import asyncio
import json
from pathlib import Path

import aiohttp


async def upload_seed_data(client: aiohttp.ClientSession, api_base_url: str):
    seed_data = json.loads((Path(__file__).parent / "seed-data.json").read_text())

    single_index_set_urls = seed_data["single_index_set_urls"]
    dual_index_set_urls = seed_data["dual_index_set_urls"]

    single_index_set_tasks = []
    dual_index_set_tasks = []
    async with asyncio.TaskGroup() as tg:
        for url in single_index_set_urls:
            single_index_set_tasks.append(tg.create_task(client.get(url)))
        for url in dual_index_set_urls:
            dual_index_set_tasks.append(tg.create_task(client.get(url)))

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for t in single_index_set_tasks:
            response = t.result()
            data = await response.json()
            tasks.append(
                tg.create_task(
                    client.post(f"{api_base_url}/index-sets/single", json=data)
                )
            )

        for t in dual_index_set_tasks:
            response = t.result()
            data = await response.json()
            tasks.append(
                tg.create_task(
                    client.post(f"{api_base_url}/index-sets/dual", json=data)
                )
            )

    for t in tasks:
        response = t.result()
        if not response.ok:
            try:
                as_json = await response.json()
                if "duplicate" not in as_json["error"]["message"]:
                    print(as_json)
            except:
                print(await response.text())

    assays = seed_data["tenx_assays"]

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for a in assays:
            tasks.append(
                tg.create_task(client.post(f"{api_base_url}/10x-assays", json=a))
            )

    for t in tasks:
        response: aiohttp.ClientResponse = t.result()
        if not response.ok:
            try:
                as_json = await response.json()
                if "duplicate" not in as_json["error"]["message"]:
                    print(as_json)
            except:
                print(await response.text())

    multiplexing_tags = seed_data["multiplexing_tags"]

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for t in multiplexing_tags:
            tasks.append(
                tg.create_task(client.post(f"{api_base_url}/multiplexing-tags", json=t))
            )

    for t in tasks:
        response: aiohttp.ClientResponse = t.result()
        if not response.ok:
            try:
                as_json = await response.json()
                if "duplicate" not in as_json["error"]["message"]:
                    print(as_json)
            except:
                print(await response.text())
