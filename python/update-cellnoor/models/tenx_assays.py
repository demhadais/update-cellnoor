import asyncio
import json
from pathlib import Path
from re import L

import aiohttp


async def upload_tenx_assays(client: aiohttp.ClientSession, api_base_url: str):
    single_index_set_urls = [
        "https://cdn.10xgenomics.com/raw/upload/v1655155349/support/in-line%20documents/Single_Index_Kit_N_Set_A.json",
        "https://cdn.10xgenomics.com/raw/upload/v1655155616/support/in-line%20documents/Single_Index_Kit_T_Set_A.json",
    ]
    dual_index_set_urls = [
        "https://cdn.10xgenomics.com/raw/upload/v1655155126/support/in-line%20documents/Dual_Index_Kit_TS_Set_A.json",
        "https://cdn.10xgenomics.com/raw/upload/v1655151898/support/in-line%20documents/Dual_Index_Kit_TT_Set_A.json",
        "https://cdn.10xgenomics.com/raw/upload/v1655156423/support/in-line%20documents/Dual_Index_Kit_NT_Set_A.json",
        "https://cdn.10xgenomics.com/raw/upload/v1655156427/support/in-line%20documents/Dual_Index_Kit_NN_Set_A.json",
        "https://cdn.10xgenomics.com/raw/upload/v1655156218/support/in-line%20documents/Dual_Index_Kit_TN_Set_A.json",
    ]

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
            print(await response.text())

    assays = json.loads((Path(__file__).parent / "tenx-assays.json").read_text())

    for a in assays:
        response = await client.post(f"{api_base_url}/10x-assays", json=a)
        if response.status == 422:
            json_response = await response.json()
            print(json_response)
